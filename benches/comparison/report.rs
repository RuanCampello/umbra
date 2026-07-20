//! Aggregates the timed cells into the markdown report: one table per category
//! with a `Best` column (fastest median wins)
use crate::harness::Timing;
use crate::scenario::Category;

pub struct Cell {
    pub timing: Option<Timing>,
    pub rows: Option<usize>,
}

pub struct Row {
    pub category: Category,
    pub label: String,
    pub cells: Vec<Cell>,
}

pub struct Report {
    pub engines: Vec<String>,
    pub rows: Vec<Row>,
    pub dataset_rows: usize,
}

const CATEGORIES: [Category; 3] = [Category::Basic, Category::Advanced, Category::Bottleneck];

impl Report {
    pub fn render(&self) -> String {
        let mut out = String::new();
        out.push_str("# Umbra comparison benchmark\n\n");
        out.push_str(&format!(
            "Median microseconds per operation (± median absolute deviation), {} users / \
             {} orders, single connection, each statement parsed and executed end to end. \
             Lower is better, **Best** is the fastest median in the row.\n\n\
             Read this with the setup in mind:\n\
             - Umbra and SQLite run **in memory**, Postgres is the local **on-disk** server, so \
             it pays client/server round-trips on every call and a durable fsync on every write \
             — its write rows are not comparable to the embedded engines'.\n\
             - Every engine indexes `age`/`status`/`user_id`, so column-filtered scans are a \
             like-for-like comparison.\n\n",
            self.dataset_rows,
            self.dataset_rows * 3,
        ));

        let mut totals = vec![0usize; self.engines.len()];
        for category in CATEGORIES {
            if !self.rows.iter().any(|row| row.category == category) {
                continue;
            }
            self.render_category(&mut out, category, &mut totals);
        }

        self.render_summary(&mut out, &totals);
        self.render_correctness(&mut out);
        out
    }

    fn render_category(&self, out: &mut String, category: Category, totals: &mut [usize]) {
        out.push_str(&format!("## {}\n\n", category.title()));

        out.push_str("| Operation |");
        for engine in &self.engines {
            out.push_str(&format!(" {engine} (µs) |"));
        }
        out.push_str(" Best |\n|:----------|");
        for _ in &self.engines {
            out.push_str("----------:|");
        }
        out.push_str(":-----|\n");

        let mut wins = vec![0usize; self.engines.len()];
        for row in self.rows.iter().filter(|row| row.category == category) {
            out.push_str(&format!("| {} |", row.label));
            for cell in &row.cells {
                match &cell.timing {
                    Some(timing) => out.push_str(&format!(
                        " {:.2} ±{:.2} |",
                        timing.median_us, timing.spread_us
                    )),
                    None => out.push_str(" — |"),
                }
            }

            match row.best() {
                Some(index) => {
                    wins[index] += 1;
                    totals[index] += 1;
                    out.push_str(&format!(" {} |\n", self.engines[index]));
                }
                None => out.push_str(" — |\n"),
            }
        }

        out.push_str(&format!(
            "\n**{} score: {}**\n\n",
            category.title(),
            self.tally(&wins)
        ));
    }

    fn render_summary(&self, out: &mut String, totals: &[usize]) {
        out.push_str("## Summary\n\n");
        out.push_str("| Engine | Wins |\n|:-------|-----:|\n");
        for (engine, total) in self.engines.iter().zip(totals) {
            out.push_str(&format!("| {engine} | {total} |\n"));
        }
        out.push('\n');
    }

    fn render_correctness(&self, out: &mut String) {
        let mut mismatches = Vec::new();
        for row in &self.rows {
            let counts: Vec<usize> = row.cells.iter().filter_map(|cell| cell.rows).collect();
            if counts.len() > 1 && counts.iter().any(|count| *count != counts[0]) {
                mismatches.push(row);
            }
        }

        out.push_str("## Correctness\n\n");
        if mismatches.is_empty() {
            out.push_str("Every compared read returned the same row count across engines.\n");
            return;
        }

        out.push_str("These reads disagreed on row count, treat their timings with suspicion:\n\n");
        for row in mismatches {
            let counts: Vec<_> = self
                .engines
                .iter()
                .zip(&row.cells)
                .map(|(engine, cell)| match cell.rows {
                    Some(rows) => format!("{engine} {rows}"),
                    None => format!("{engine} —"),
                })
                .collect();
            out.push_str(&format!("- {}: {}\n", row.label, counts.join(", ")));
        }
    }

    fn tally(&self, wins: &[usize]) -> String {
        self.engines
            .iter()
            .zip(wins)
            .map(|(engine, count)| format!("{engine} {count}"))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl Row {
    fn best(&self) -> Option<usize> {
        self.cells
            .iter()
            .enumerate()
            .filter_map(|(index, cell)| {
                cell.timing.as_ref().map(|timing| (index, timing.median_us))
            })
            .min_by(|a, b| a.1.partial_cmp(&b.1).expect("medians are never NaN"))
            .map(|(index, _)| index)
    }
}
