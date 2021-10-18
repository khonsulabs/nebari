//! An example on how to use `nebari::tree`, the low-level single-tree database
//! layer. For most users, the `nebari::Roots` type should be preferred.

use nebari::{
    io::{fs::StdFile, memory::MemoryFile, ManagedFile},
    tree::{KeyEvaluation, State, TreeFile, Unversioned, Versioned},
    Buffer, Context, Error,
};

fn main() -> Result<(), Error> {
    // At its core, Nebari operates on TreeFiles. Each TreeFile is defined by
    // its Root -- the two Roots included with Nebari are the VersionedTreeRoot
    // and UnversionedTreeRoot. As you may guess from their names, the different
    // tree roots control whether full revision information is stored.
    //
    // To begin, let's open up a file without versioning on the filesystem:
    let mut unversioned_tree = TreeFile::<Unversioned, StdFile>::write(
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
    let mut unversioned_memory_tree = TreeFile::<Unversioned, MemoryFile>::write(
        "unversioned-tree.nebari",
        State::default(),
        &Context::default(),
        None,
    )?;
    tree_basics(&mut unversioned_memory_tree)?;

    // We can also use the VersionedTreeRoot to be able to view full revision history in a tree.
    let mut versioned_tree = TreeFile::<Versioned, StdFile>::write(
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
        &mut |_key| KeyEvaluation::ReadData,
        &mut |key, data| {
            println!(
                "Key {:?} contained {:?} at sequence {:?}. Previous sequence: {:?}",
                key.key, data, key.sequence, key.last_sequence
            );
            Ok(())
        },
    )?;

    Ok(())
}

fn tree_basics<Root: nebari::tree::Root, File: ManagedFile>(
    tree: &mut TreeFile<Root, File>,
) -> Result<(), Error> {
    // Insert a few key-value pairs.
    tree.push(None, Buffer::from(b"a key"), Buffer::from(b"a value"))?;
    tree.push(None, Buffer::from(b"b key"), Buffer::from(b"b value"))?;
    // Retrieve a value.
    let value = tree.get(b"a key", false)?.unwrap();
    assert_eq!(value, b"a value");

    // Scan for values
    tree.scan(
        ..,
        true,
        false,
        &mut |_, _, _| true,
        &mut |_key, _index| KeyEvaluation::ReadData,
        &mut |key, _index, value| {
            println!("Found key {:?} with value {:?}", key, value);
            Ok(())
        },
    )?;

    // Replace the value for "a key"
    tree.push(None, Buffer::from(b"a key"), Buffer::from(b"a new value"))?;
    let updated_value = tree.get(b"a key", false)?.unwrap();
    assert_eq!(updated_value, b"a new value");

    Ok(())
}
