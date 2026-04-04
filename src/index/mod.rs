mod indexer;
mod schema;
mod state;
mod sync;

pub use indexer::{discover_and_sort_files, gc_stale_entries, index_files, IndexProgress};
pub use schema::SessionIndex;
pub use state::IndexState;
pub use sync::ensure_index_fresh;
