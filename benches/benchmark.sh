#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

DATASET="${DATASET:-"$SCRIPT_DIR/dataset.sql"}"
UMBRA_DB="${UMBRA_DB:-/tmp/umbra_bench.db}"
UMBRA_PORT="${UMBRA_PORT:-8000}"
POSTGRES_URL="${POSTGRES_URL:-postgres://postgres:postgres@localhost:5432/umbra_bench}"
RUNS="${RUNS:-5}"
HYPERFINE_BIN="${HYPERFINE_BIN:-hyperfine}"
UMBRA_BIN="${UMBRA_BIN:-cargo run --quiet --release --}"
USQL_BIN="${USQL_BIN:-cargo run --quiet --release --package usql --}"

UMBRA_PID=""
UMBRA_LOG=""
UMBRA_LOAD_LOG=""
POSTGRES_LOAD_LOG=""
KEEP_UMBRA_LOAD_LOG=""
KEEP_POSTGRES_LOAD_LOG=""

require_bin() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required dependency: $1" >&2
    exit 1
  fi
}

cleanup() {
  if [[ -n "$UMBRA_PID" ]] && kill -0 "$UMBRA_PID" 2>/dev/null; then
    kill "$UMBRA_PID" 2>/dev/null || true
    wait "$UMBRA_PID" 2>/dev/null || true
  fi
  if [[ -n "$UMBRA_LOG" && -f "$UMBRA_LOG" ]]; then
    echo "Umbra server log kept at $UMBRA_LOG"
  fi
  if [[ -z "$KEEP_UMBRA_LOAD_LOG" && -n "$UMBRA_LOAD_LOG" && -f "$UMBRA_LOAD_LOG" ]]; then
    rm -f "$UMBRA_LOAD_LOG"
  fi
  if [[ -z "$KEEP_POSTGRES_LOAD_LOG" && -n "$POSTGRES_LOAD_LOG" && -f "$POSTGRES_LOAD_LOG" ]]; then
    rm -f "$POSTGRES_LOAD_LOG"
  fi
}
trap cleanup EXIT

require_bin psql
require_bin "$HYPERFINE_BIN"

if [[ ! -f "$DATASET" ]]; then
  echo "Dataset not found at $DATASET" >&2
  exit 1
fi

wait_for_umbra() {
  for _ in {1..40}; do
    if (echo >/dev/tcp/127.0.0.1/"$UMBRA_PORT") 2>/dev/null; then
      return 0
    fi
    sleep 0.5
  done

  echo "Umbra server did not become ready on port $UMBRA_PORT" >&2
  exit 1
}

echo "Using dataset: $DATASET"
echo "PostgreSQL URL: $POSTGRES_URL"
echo "Umbra DB: $UMBRA_DB (port $UMBRA_PORT)"

UMBRA_LOG="$(mktemp -t umbra-server.XXXXXX.log)"
echo "Starting Umbra server (logging to $UMBRA_LOG)..."
$UMBRA_BIN "$UMBRA_DB" "$UMBRA_PORT" >"$UMBRA_LOG" 2>&1 &
UMBRA_PID=$!
wait_for_umbra

echo "Loading dataset into Umbra..."
UMBRA_LOAD_LOG="$(mktemp -t umbra-load.XXXXXX.log)"
if ! $USQL_BIN "$UMBRA_PORT" <"$DATASET" >"$UMBRA_LOAD_LOG" 2>&1; then
  echo "Failed to load dataset into Umbra (see $UMBRA_LOAD_LOG)" >&2
  sed 's/^/[umbra] /' "$UMBRA_LOAD_LOG" >&2 || true
  KEEP_UMBRA_LOAD_LOG=1
  exit 1
fi
rm -f "$UMBRA_LOAD_LOG"
UMBRA_LOAD_LOG=""

echo "Loading dataset into PostgreSQL..."
POSTGRES_LOAD_LOG="$(mktemp -t postgres-load.XXXXXX.log)"
if ! psql "$POSTGRES_URL" -v ON_ERROR_STOP=1 -f "$DATASET" >"$POSTGRES_LOAD_LOG" 2>&1; then
  echo "Failed to load dataset into PostgreSQL (see $POSTGRES_LOAD_LOG)" >&2
  sed 's/^/[postgres] /' "$POSTGRES_LOAD_LOG" >&2 || true
  KEEP_POSTGRES_LOAD_LOG=1
  exit 1
fi
rm -f "$POSTGRES_LOAD_LOG"
POSTGRES_LOAD_LOG=""

bench_query() {
  local name="$1"
  local sql="$2"
  local tmp_dir
  tmp_dir="$(mktemp -d -t umbra-bench.XXXXXX)"

  local pg_sql="$tmp_dir/query.sql"
  local umbra_sql="$tmp_dir/query.usql"

  printf "%s\n" "$sql" >"$pg_sql"
  printf "%s\n/quit\n" "$sql" >"$umbra_sql"

  local pg_cmd="bash -lc 'psql \"$POSTGRES_URL\" -v ON_ERROR_STOP=1 -f \"$pg_sql\" >/dev/null'"
  local umbra_cmd="bash -lc '$USQL_BIN \"$UMBRA_PORT\" < \"$umbra_sql\" >/dev/null'"

  echo ""
  echo "Benchmarking $name..."
  $HYPERFINE_BIN --warmup 1 --runs "$RUNS" \
    --command-name "umbra-$name" "$umbra_cmd" \
    --command-name "postgres-$name" "$pg_cmd"

  rm -rf "$tmp_dir"
}

QUERY_ORDER_TOTALS="
SELECT country,
       COUNT(o.order_id) AS order_count,
       SUM(o.total_amount) AS gross_revenue
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY country
ORDER BY gross_revenue DESC
LIMIT 1000;"

QUERY_BASKET_SIZE="
SELECT o.order_id,
       o.order_date,
       SUM(oi.quantity * oi.item_price) AS basket_total
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.status = 'completed'
  AND o.order_date BETWEEN '2022-01-01' AND '2023-12-31'
GROUP BY o.order_id, o.order_date
ORDER BY basket_total DESC
LIMIT 5000;"

QUERY_CATEGORY_MIX="
SELECT p.category,
       SUM(oi.quantity) AS units_sold,
       SUM(oi.quantity * oi.item_price) AS category_revenue
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
GROUP BY p.category
ORDER BY category_revenue DESC;"

bench_query "country-aggregation" "$QUERY_ORDER_TOTALS"
bench_query "basket-tops" "$QUERY_BASKET_SIZE"
bench_query "category-mix" "$QUERY_CATEGORY_MIX"

echo ""
echo "Benchmark complete."
