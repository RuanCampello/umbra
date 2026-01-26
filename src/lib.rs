//! # Umbra Database Architecture
//!
//! Umbra is an embeddable, relational database built from scratch in Rust. It follows a classic
//! database architecture split into two main components:
//!
//! 1. **Storage Engine (`core::storage`)**: Manages data persistence, memory caching, and low-level data structures.
//!    - Inspired by SQLite and PostgreSQL.
//!    - Uses **Slotted Pages** (4KB default) as the fundamental unit of storage.
//!    - Implements a **B+Tree** for tables and indexes.
//!    - Manages concurrency and IO via a **Pager** and **Buffer Pool** (Cache).
//!
//! 2. **Query Engine (`sql` & `vm`)**: Handles the interpretation and execution of SQL queries.
//!    - **Parsing**: Tokenizes and parses SQL into an Abstract Syntax Tree (AST).
//!    - **Analysis**: Validates the AST against the system catalog (schema).
//!    - **Optimisation**: Simplifies expressions and selects index strategies.
//!    - **Execution**: Uses a Volcano-style iterator model (pull-based) to execute the plan.
//!
//! ## Key Modules
//! - [`sql`]: The SQL compilation pipeline (Text -> AST -> Plan).
//! - [`vm`]: The Virtual Machine (actually a plan-tree) that executes the query plans.
//! - [`core`]: The Storage Engine, Type System, and foundational utilities.
//! - [`db`]: The user-facing API and transaction management.

use db::DatabaseError;

mod collections;
mod core;
pub mod db;
pub(crate) mod os;
pub mod sql;
pub mod tcp;
mod vm;

pub type Result<T> = std::result::Result<T, DatabaseError>;
