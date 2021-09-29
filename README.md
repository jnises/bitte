# Bitte

Small web server that provides directory listings and presigned urls to an s3 bucket.

## Dependencies
* Rust (https://rustup.rs/)

## How to start
To build:
```shell
cargo build --release
```

To get info on available arguments:
```shell
./target/release/bitte --help
```

You can test using minio running locally using something like `cargo run -- --endpoint http://localhost:9000 --bucket asdf`

