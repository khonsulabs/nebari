#![no_main]
use std::collections::{BTreeMap, BTreeSet};

use libfuzzer_sys::fuzz_target;
use nebari::{
    io::{fs::StdFile, FileManager},
    tree::{
        CompareSwap, KeyOperation, Modification, Operation, PersistenceMode, State, TreeFile,
        Unversioned,
    },
    ArcBytes, Context,
};
use tempfile::NamedTempFile;

fuzz_target!(|batches: Vec<BTreeSet<u16>>| {
    let context = Context::default();
    let file = NamedTempFile::new().unwrap();
    let mut tree =
        TreeFile::<Unversioned, StdFile>::write(&file, State::default(), &context, None).unwrap();

    let mut oracle = BTreeMap::new();
    let ops = batches.iter().map(|b| b.len()).sum::<usize>();
    if ops > 1000 {
        let keys = batches.iter().flatten().collect::<BTreeSet<_>>().len();
        println!("{ops} operations, {keys} keys");
    }
    for batch in batches {
        tree.modify(Modification {
            persistence_mode: PersistenceMode::Sync,
            keys: batch
                .iter()
                .map(|key| ArcBytes::from(key.to_be_bytes()))
                .collect(),
            operation: Operation::CompareSwap(CompareSwap::new(
                &mut |key, _index, current_value| {
                    if current_value.is_some() {
                        KeyOperation::Remove
                    } else {
                        KeyOperation::Set(key.to_owned())
                    }
                },
            )),
        })
        .unwrap();
        for key in batch {
            oracle
                .entry(ArcBytes::from(key.to_be_bytes()))
                .and_modify(|present: &mut bool| *present = !*present)
                .or_insert(true);
        }
    }
    // Test a scan
    for (key, _) in tree.get_range(&(..), false).unwrap() {
        assert_eq!(
            oracle.get(&key),
            Some(&true),
            "{key:?} was present but shouldn't have been"
        );
    }

    // Test getting via keys
    for (key, _) in tree
        .get_multiple(oracle.keys().map(ArcBytes::as_slice), false)
        .unwrap()
    {
        if let Some(should_be_present) = oracle.remove(&key) {
            assert!(
                should_be_present,
                "oracle says {key:?} shouldn't be present"
            );
        } else {
            unreachable!("{key:?} was not in oracle");
        }
    }

    if let Some(bad_key) = oracle
        .iter()
        .find(|(_key, should_be_present)| **should_be_present)
    {
        unreachable!("oracle says {bad_key:?} should be present, but it was not found");
    }

    drop(tree);
    context.file_manager.delete(&file).unwrap();
});
