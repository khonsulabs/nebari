//! An example on how to use `nebari::tree`, the low-level single-tree database
//! layer. For most users, the `nebari::Roots` type should be preferred.

use nebari::{
    tree::{ScanEvaluation, State, TreeFile, Unversioned, Versioned},
    ArcBytes, Context, Error,
};
use sediment::io::{fs::StdFileManager, memory::MemoryFileManager, FileManager};

fn main() -> Result<(), Error> {
    // At its core, Nebari operates on TreeFiles. Each TreeFile is defined by
    // its Root -- the two Roots included with Nebari are the VersionedTreeRoot
    // and UnversionedTreeRoot. As you may guess from their names, the different
    // tree roots control whether full revision information is stored.
    //
    // To begin, let's open up a file without versioning on the filesystem:
    let mut unversioned_tree = TreeFile::<Unversioned, StdFileManager>::open(
        "unversioned-tree.nebari",
        State::default(),
        &Context::default(),
        None,
    )?;

    // We can use this filesystem-backed tree to do common tree operations.
    tree_basics(&mut unversioned_tree)?;

    // Or we could do the same thing, but this time using a memory-backed file.
    // Note: this isn't recommended to be used in production, but it can be
    // helpful in CI environments.
    let mut unversioned_memory_tree = TreeFile::<Unversioned, MemoryFileManager>::open(
        "unversioned-tree.nebari",
        State::default(),
        &Context::default(),
        None,
    )?;
    tree_basics(&mut unversioned_memory_tree)?;

    // We can also use the VersionedTreeRoot to be able to view full revision history in a tree.
    let mut versioned_tree = TreeFile::<Versioned, StdFileManager>::open(
        "versioned-tree.nebari",
        State::default(),
        &Context::default(),
        None,
    )?;
    tree_basics(&mut versioned_tree)?;

    versioned_tree.scan_sequences(
        ..,
        true,
        false,
        |_key| ScanEvaluation::ReadData,
        |key, data| {
            println!(
                "Key {:?} contained {:?} at sequence {:?}. Previous sequence: {:?}",
                key.key, data, key.sequence, key.last_sequence
            );
            Ok(())
        },
    )?;

    Ok(())
}

fn tree_basics<Root: nebari::tree::Root<Value = ArcBytes<'static>>, File: FileManager>(
    tree: &mut TreeFile<Root, File>,
) -> Result<(), Error> {
    // Insert a few key-value pairs.
    tree.set(None, ArcBytes::from(b"a key"), ArcBytes::from(b"a value"))?;
    tree.set(None, ArcBytes::from(b"b key"), ArcBytes::from(b"b value"))?;
    // Retrieve a value.
    let value = tree.get(b"a key", false)?.unwrap();
    assert_eq!(value, b"a value");

    // Scan for values
    tree.scan(
        &(..),
        true,
        false,
        |_, _, _| ScanEvaluation::ReadData,
        |_key, _index| ScanEvaluation::ReadData,
        |key, _index, value| {
            println!("Found key {:?} with value {:?}", key, value);
            Ok(())
        },
    )?;

    // Replace the value for "a key"
    tree.set(
        None,
        ArcBytes::from(b"a key"),
        ArcBytes::from(b"a new value"),
    )?;
    let updated_value = tree.get(b"a key", false)?.unwrap();
    assert_eq!(updated_value, b"a new value");

    Ok(())
}
