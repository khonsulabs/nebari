use std::convert::Infallible;

use byteorder::BigEndian;
use nanorand::{Pcg64, Rng};
use nebari::{
    tree::{EmbeddedIndex, Reducer, Root, ScanEvaluation, Serializable, VersionedTreeRoot},
    Error,
};

fn main() -> Result<(), Error> {
    let roots = nebari::Config::default_for("embedded-indexes.nebari").open()?;

    // Nebari provides a way to embed data within the B-Tree directly through a
    // `EmbeddedIndex` and `EmbeddedStats` generics on either
    // `VersionedTreeRoot` or `UnversionedTreeRoot`.
    let tree = roots.tree(VersionedTreeRoot::<Zeroes>::tree("one"))?;
    let mut rng = Pcg64::new();
    // This example counts the number of '0' characters and stores it in the index.
    for i in 0_u32..100 {
        tree.set(i.to_be_bytes(), rng.generate::<u64>().to_string())?;
    }

    // When scanning the tree, we can retrieve our embedded statistics during
    // the scan at every level of the tree, and we have full control over
    // iteration of the tree.
    tree.scan::<Infallible, _, _, _, _>(
        &(..),
        true,
        // The node evaluator is called for every interior node -- nodes that
        // don't contain values directly, but point to other nodes instead.
        // Returning true allows the scan to dive into the node.
        |max_key, index, depth| {
            println!(
                "Interior found with a maximum key stored of {:?} at depth {} with {} zeros",
                max_key, depth, index.embedded.0,
            );
            ScanEvaluation::ReadData
        },
        // The key evaluator is called for each key discovered in the tree, but
        // before the stored value is read.
        |key, index| {
            println!("Key {:?} has {} zeros", key, index.embedded.0);
            ScanEvaluation::Skip
        },
        // The data callback is invoked once data is read. In this example, we
        // always skip reading, so this is unreachable.
        |_key, _index, _value| unreachable!(),
    )?;

    Ok(())
}

#[derive(Clone, Debug)]
pub struct Zeroes(pub u32);

impl EmbeddedIndex for Zeroes {
    type Reduced = Self;
    type Reducer = ZeroesReducer;

    fn index(_key: &nebari::ArcBytes<'_>, value: Option<&nebari::ArcBytes<'static>>) -> Self {
        Self(
            value
                .map(|bytes| bytes.iter().filter(|&b| b as char == '0').count())
                .unwrap_or_default() as u32,
        )
    }
}

#[derive(Default, Clone, Debug)]
pub struct ZeroesReducer;

impl Reducer<Zeroes> for ZeroesReducer {
    fn reduce<'a, Indexes, IndexesIter>(&self, indexes: Indexes) -> Zeroes
    where
        Indexes: IntoIterator<Item = &'a Zeroes, IntoIter = IndexesIter> + ExactSizeIterator,
        IndexesIter: Iterator<Item = &'a Zeroes> + ExactSizeIterator + Clone,
    {
        Zeroes(indexes.into_iter().map(|i| i.0).sum())
    }

    fn rereduce<'a, ReducedIndexes, ReducedIndexesIter>(&self, values: ReducedIndexes) -> Zeroes
    where
        Self: 'a,
        ReducedIndexes:
            IntoIterator<Item = &'a Zeroes, IntoIter = ReducedIndexesIter> + ExactSizeIterator,
        ReducedIndexesIter: Iterator<Item = &'a Zeroes> + ExactSizeIterator + Clone,
    {
        // TODO change reduce to an iterator too
        self.reduce(values)
    }
}

impl Serializable for Zeroes {
    fn serialize_to<W: byteorder::WriteBytesExt>(&self, writer: &mut W) -> Result<usize, Error> {
        writer.write_u32::<BigEndian>(self.0)?;
        Ok(std::mem::size_of::<u32>())
    }

    fn deserialize_from<R: byteorder::ReadBytesExt>(reader: &mut R) -> Result<Self, Error> {
        Ok(Self(reader.read_u32::<BigEndian>()?))
    }
}
