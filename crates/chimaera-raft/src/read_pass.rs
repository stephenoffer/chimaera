//! Reads that share a view of each group's state, within one pass of work on
//! one thread (`ReadPass`).
//!
//! `Group::read` opens a redb read transaction and its table for each read,
//! and a peer's core makes several reads for each call it places, grants or
//! dispatches: under a burst, opening them was about half the core's read
//! time. While a pass is active on a thread, that thread's reads of a group
//! share one view, opened at its first read: it holds everything applied
//! before then, and nothing applied since. So a pass covers work that reads
//! only what was applied before it began (the core handling one message: the
//! event for an apply is sent once the apply commits). The guard cannot be
//! held across an `.await` (it is not `Send`): a read transaction held long
//! pins pages redb would otherwise reuse.

use crate::state_machine::GroupRead;
use redb::Database;
use std::cell::RefCell;
use std::marker::PhantomData;

thread_local! {
    /// The active pass's views: (database, group) and the view, or None.
    static VIEWS: RefCell<Option<Vec<(usize, u32, GroupRead)>>> = const { RefCell::new(None) };
}

/// Reads on this thread share their views until it is dropped. A pass begun
/// inside another changes nothing.
pub struct ReadPass {
    owner: bool,
    _not_send: PhantomData<*const ()>,
}

impl ReadPass {
    pub fn begin() -> ReadPass {
        let owner = VIEWS.with(|v| {
            let mut v = v.borrow_mut();
            v.is_none().then(|| *v = Some(Vec::new())).is_some()
        });
        ReadPass { owner, _not_send: PhantomData }
    }
}

impl Drop for ReadPass {
    fn drop(&mut self) {
        if self.owner {
            VIEWS.with(|v| v.borrow_mut().take());
        }
    }
}

/// `f` over the pass's view of `group` in `db` (opened at its first read),
/// or None when no pass is active on this thread.
pub(crate) fn with<T>(db: &Database, group: u32, f: impl FnOnce(&GroupRead) -> T) -> Option<std::io::Result<T>> {
    let key = db as *const Database as usize;
    // Taken out while `f` runs: a read of the same group inside it opens its own.
    let found = VIEWS.with(|v| {
        let mut v = v.borrow_mut();
        let views = v.as_mut()?;
        Some(views.iter().position(|(d, g, _)| *d == key && *g == group).map(|i| views.swap_remove(i).2))
    })?;
    let view = match found {
        Some(view) => view,
        None => match GroupRead::open(db, group) {
            Ok(view) => view,
            Err(e) => return Some(Err(e)),
        },
    };
    let out = f(&view);
    VIEWS.with(|v| {
        if let Some(views) = v.borrow_mut().as_mut() {
            // In place of one a read inside `f` opened: the outermost read
            // returns last, so the pass keeps its first view.
            views.retain(|(d, g, _)| !(*d == key && *g == group));
            views.push((key, group, view));
        }
    });
    Some(Ok(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_machine::{SM, with_prefix};
    use chimaera_core::kv::KvRead;

    fn put(db: &Database, group: u32, key: &[u8], value: &[u8]) {
        let txn = db.begin_write().unwrap();
        txn.open_table(SM).unwrap().insert(with_prefix(group, key).as_slice(), value).unwrap();
        txn.commit().unwrap();
    }

    fn get(db: &Database, group: u32, key: &[u8]) -> Option<Vec<u8>> {
        match with(db, group, |v| v.get(key)) {
            Some(r) => r.unwrap(),
            None => GroupRead::open(db, group).unwrap().get(key),
        }
    }

    #[test]
    fn reads_in_a_pass_share_a_view_of_what_was_applied_before_it() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path().join("sm.redb")).unwrap();
        put(&db, 3, b"before", b"1");
        assert!(with(&db, 3, |_| ()).is_none(), "no pass: each read opens its own view");
        {
            let _pass = ReadPass::begin();
            assert_eq!(get(&db, 3, b"before"), Some(b"1".to_vec()), "a write applied before the pass is seen");
            put(&db, 3, b"during", b"2");
            // The same view: nothing applied since the pass's first read.
            assert_eq!(get(&db, 3, b"during"), None);
            // A read inside a read of the same group opens its own view.
            let nested = with(&db, 3, |_| get(&db, 3, b"during")).unwrap().unwrap();
            assert_eq!(nested, Some(b"2".to_vec()));
            let _inner = ReadPass::begin(); // nested: the outer pass's views stay
            assert_eq!(get(&db, 3, b"during"), None);
        }
        assert_eq!(get(&db, 3, b"during"), Some(b"2".to_vec()), "after the pass, a read sees the latest");
    }
}
