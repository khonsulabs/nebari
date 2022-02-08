use nebari::{
    tree::{Root, Versioned},
    Error,
};

fn main() -> Result<(), Error> {
    // The Roots type creates a multi-"tree" database by creating a folder at
    // the path provided and creating individual files within. While the
    // nomenclature "file" is used, Nebari is written atop an abstraction layer
    // that allows using a memory-backed simulated file system and could be used
    // to implement non-traditional storage backends.
    let roots = nebari::Config::default_for("simple-database.nebari").open()?;

    // Each tree contains a unique set of keys and values, and can be written
    // using a versioned or unversioned tree root.
    let tree_one = roots.tree(Versioned::tree("one"))?;
    tree_one.set("hello", "world")?;
    let tree_two = roots.tree(Versioned::tree("two"))?;
    assert!(tree_two.get("hello".as_bytes())?.is_none());

    // Each operation on a Tree is executed within an ACID-compliant
    // transaction. If you want to execute multiple operations in a single
    // transaction, you can do that as well:
    let mut transaction = roots.transaction(&[Versioned::tree("one"), Versioned::tree("two")])?;

    // This API isn't as ergonomic as it should be. The trees are accessible in
    // the same order in which they're specified in the transaction call:
    let tx_tree_one = transaction.tree::<Versioned>(0).unwrap();
    tx_tree_one.set("hello", "everyone")?;

    // The operation above is not visible to trees outside of the transaction:
    assert_eq!(
        tree_one.get("hello".as_bytes())?.as_deref(),
        Some("world".as_bytes())
    );

    let tx_tree_two = transaction.tree::<Versioned>(1).unwrap();
    tx_tree_two.set("another tree", "another value")?;

    // If you drop the transaction before calling commit, the transaction will be rolled back.
    transaction.commit()?;

    // Now the changes are visible:
    assert_eq!(
        tree_one.get("hello".as_bytes())?.as_deref(),
        Some("everyone".as_bytes())
    );

    Ok(())
}
