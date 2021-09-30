# Nebari Benchmarks

This benchmark suite is aimed at measuring the functionality that Nebari
provides. This suite is not meant to try to compare database engines, as Nebari
lacks many features of other engines used in this suite. We include those other
databases as ways to inform our development as to where Nebari isn't performing
as well as it should relative to other engines.

Our goal is to write a pure-rust, 100% safe, reasonably efficient,
easy-to-maintain, ACID-compliant database. We are not aiming to be the best in
any given metric or category.

## Viewing Benchmarks

As Nebari development progresses, we occasionally run this benchmark suite on a
VPS similar to what we expect to run [Cosmic Verge][cv] on someday. You can view
the [latest results here.][benches-vps]. Additionally, benchmarks are
[executed][benches-yml] with Github Actions and [published here][benches-gha].

## Executing Benchmarks

All cargo commands can be executed at the workspace/repository root.

To execute the Nebari benchmarks alone, simply execute `cargo bench`. This will
produce an HTML report in `target/criterion/report/`. 

To enable additional database engines, enable the feature flags desired: `cargo
bench --features sqlite,sled,persy`.

To generate the overview in `target/criterion`, execute `cargo xtask
generate-benchmark-overview`.

## Contributing

We would be extremely grateful if you wish to contribute additional benchmarks
that measure functionality of Nebari not currently being measured. We are not
actively looking to add any additional databases to this benchmark suite.


[cv]: https://github.com/khonsulabs/cosmicverge
[benches-vps]: https://khonsulabs-storage.s3.us-west-000.backblazeb2.com/nebari-scaleway-gp1-xs/index.html
[benches-yml]: ../.github/workflows/benches.yml
[benches-gha]: https://nebari.bonsaidb.io/benchmarks/