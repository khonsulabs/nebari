#!/bin/bash
#
# Most hosting providers offer a way to initialize an instance with a startup
# script. This script will execute benchmarks, and if provided credentials, will
# upload the results to Backblaze B2 using their client.


# Install build-essential for compilation, and sqlite3 for benchmarks.
apt-get update
apt-get install -y build-essential libsqlite3-dev

# Install the current stable rust version with default options
curl https://sh.rustup.rs -sSf | sh -s -- -y
source /root/.cargo/env

# Run the benchmarks
git clone https://github.com/khonsulabs/nebari.git
cd nebari
cargo bench --features sled
mv target/criterion ../generated-report
cd ..
rm -rf nebari

# The directory `generated-report` now contains an HTML report.

# Upload to Backblaze B2 for viewing.
curl https://github.com/Backblaze/B2_Command_Line_Tool/releases/latest/download/b2-linux -o b2-linux
chmox +x b2-linux
B2_APPLICATION_KEY_ID="keyid" B2_APPLICATION_KEY="key" ./b2-linux sync --delete generated-report b2://khonsulabs-storage/nebari-PATH