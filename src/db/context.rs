use crate::collections::hash::{BuildHasher, HashMap};
use crate::db::DatabaseError;
use crate::db::{Ctx, SqlError, TableMetadata};

#[derive(Debug)]
pub(crate) struct Context {
    /// Maps table name to the index in the `entries` vector.
    map: HashMap<String, usize>,
    /// Arena storage for the linked list nodes.
    entries: Vec<Option<Entry>>,
    /// Index of the Most Recently Used item.
    head: Option<usize>,
    /// Index of the Least Recently Used item.
    tail: Option<usize>,
    /// Stack of free indices in `entries` to reuse memory.
    free_indices: Vec<usize>,
    /// Maximum capacity of the cache.
    capacity: usize,
}

/// Node for the `LRU` doubly linked list.
#[derive(Debug)]
struct Entry {
    key: String,
    value: TableMetadata,
    prev: Option<usize>,
    next: Option<usize>,
}

impl Context {
    #[cfg(test)]
    pub fn new() -> Self {
        use crate::db::DEFAULT_CACHE_SIZE;

        Self::with_size(DEFAULT_CACHE_SIZE)
    }

    pub fn with_size(capacity: usize) -> Self {
        Self {
            map: HashMap::with_capacity_and_hasher(capacity, BuildHasher),
            entries: Vec::with_capacity(capacity),
            head: None,
            tail: None,
            free_indices: Vec::new(),
            capacity,
        }
    }

    /// Inserts a table metadata into the context.
    /// If the table already exists, it updates it and moves it to the head (MRU).
    /// If the cache is full, it evicts the tail (LRU).
    pub fn insert(&mut self, metadata: TableMetadata) {
        let key = metadata.name.clone();

        match self.map.get(&key) {
            Some(&idx) => {
                if let Some(entry) = &mut self.entries[idx] {
                    entry.value = metadata;
                }

                self.move_to_head(idx);
            }

            _ => {
                if self.map.len() >= self.capacity {
                    self.evict_tail();
                }

                let index = match self.free_indices.pop() {
                    Some(idx) => idx,
                    _ => {
                        let idx = self.entries.len();
                        self.entries.push(None);
                        idx
                    }
                };

                let entry = Entry {
                    key: key.clone(),
                    value: metadata,
                    prev: None,
                    next: None,
                };

                self.entries[index] = Some(entry);
                self.map.insert(key, index);
                self.push_front(index);
            }
        }
    }

    /// Checks if the context contains a table.
    /// This does NOT update the LRU order (peek operation).
    pub(super) fn contains(&self, table: &str) -> bool {
        self.map.contains_key(table)
    }

    /// Removes a table from the context.
    pub fn invalidate(&mut self, table: &str) {
        if let Some(index) = self.map.remove(table) {
            self.remove_node(index);
            self.entries[index] = None;
            self.free_indices.push(index);
        }
    }

    fn move_to_head(&mut self, index: usize) {
        if self.head == Some(index) {
            return;
        }
        self.remove_node(index);
        self.push_front(index);
    }

    fn push_front(&mut self, index: usize) {
        // current head becomes next of new node
        if let Some(entry) = &mut self.entries[index] {
            entry.next = self.head;
            entry.prev = None;
        }

        match self.head {
            Some(head_idx) => {
                if let Some(head_entry) = &mut self.entries[head_idx] {
                    head_entry.prev = Some(index);
                }
            }
            _ => self.tail = Some(index),
        }

        self.head = Some(index);
    }

    fn remove_node(&mut self, index: usize) {
        let (prev, next) = {
            let entry = self.entries[index].as_ref().expect("Node should exist");
            (entry.prev, entry.next)
        };

        match prev {
            Some(prev_idx) => {
                if let Some(prev_entry) = &mut self.entries[prev_idx] {
                    prev_entry.next = next;
                }
            }
            _ => self.head = next,
        }

        match next {
            Some(next_idx) => {
                if let Some(next_entry) = &mut self.entries[next_idx] {
                    next_entry.prev = prev;
                }
            }
            _ => self.tail = prev,
        }
    }

    fn evict_tail(&mut self) {
        if let Some(tail_idx) = self.tail {
            let key_to_remove = self.entries[tail_idx].as_ref().unwrap().key.clone();
            self.remove_node(tail_idx);
            self.map.remove(&key_to_remove);
            self.entries[tail_idx] = None;
            self.free_indices.push(tail_idx);
        }
    }
}

/// Test-only implementation: clones everything for simplicity
#[cfg(test)]
impl TryFrom<&[&str]> for Context {
    type Error = DatabaseError;

    fn try_from(statements: &[&str]) -> Result<Self, Self::Error> {
        let mut context = Self::new();
        let mut root = 1;

        for sql in statements {
            use crate::sql::{
                parser::Parser,
                statement::{Create, Statement},
            };

            let statement = Parser::new(sql).parse_statement()?;
            match statement {
                Statement::Create(Create::Table { name, columns }) => {
                    use crate::db::Schema;

                    let mut schema = Schema::from(&columns);
                    schema.prepend_id();

                    let mut metadata = TableMetadata {
                        root,
                        name: name.clone(),
                        row_id: 1,
                        schema,
                        indexes: vec![],
                        serials: HashMap::default(),
                        count: 0,
                    };
                    root += 1;

                    columns.iter().for_each(|col| {
                        col.constraints.iter().for_each(|constraint| {
                            use crate::{db::IndexMetadata, index, sql::statement::Constraint};

                            let index = match constraint {
                                Constraint::Unique => index!(unique on name (col.name)),
                                Constraint::PrimaryKey => index!(primary on (name)),
                                _ => unreachable!("This ain't a index")
                            };

                            let mut index_col = col.clone();
                            let mut pk_col = columns[0].clone();

                            index_col.constraints.retain(|c| !matches!(c, Constraint::Nullable));
                            pk_col.constraints.retain(|c| !matches!(c, Constraint::Nullable));

                            metadata.indexes.push(IndexMetadata {
                                column: col.clone(),
                                schema: Schema::new(vec![index_col, pk_col]),
                                name: index,
                                root,
                                unique: true,
                            });

                            root += 1;
                        })
                    });

                    context.insert(metadata);
                }
                Statement::Create(Create::Index { name, column, unique, .. }) if unique => {
                    use crate::db::{IndexMetadata, Schema};

                    let table = context.metadata(&name)?;
                    let index_col = &table.schema.columns[table.schema.index_of(&column).unwrap()];

                    table.indexes.push(IndexMetadata{
                        column: index_col.clone(),
                        schema: Schema::new(vec![index_col.clone(), table.schema.columns[0].clone()]),
                        name,
                        root,
                        unique,
                    });

                    root += 1;
                }

                statement => {
                    return Err(SqlError::Other(format!("Only create unique index and create table should be called by test context, but found {statement:#?}")).into())
                }
            }
        }

        Ok(context)
    }
}

impl Ctx for Context {
    fn metadata(&mut self, table: &str) -> Result<&mut TableMetadata, DatabaseError> {
        self.map
            .get(table)
            .copied()
            .ok_or_else(|| SqlError::InvalidTable(table.to_string()).into())
            .map(|index| {
                self.move_to_head(index);
                &mut self.entries[index].as_mut().unwrap().value
            })
    }
}
