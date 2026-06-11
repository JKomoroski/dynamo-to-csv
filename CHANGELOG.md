# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-06-11

### Added
- Interactive mode: list tables, discover attributes from a sample scan, and select columns interactively
- Non-interactive (CLI) mode: `./SimpleTableExport.java [outputFile] [tableName] [attr1] ...`
- Parallel DynamoDB table scan using all available CPU segments via parallel streams
- Full DynamoDB type support: `S`, `N`, `BOOL`, `NULL`, `L`, `M`, `SS`, `NS`, `B`, `BS` — complex types serialized as DynamoDB JSON wire format
- `ConcurrentCSVWriter` for thread-safe, async CSV output via a bounded blocking queue
- Region resolved automatically from the AWS default provider chain (`AWS_REGION`, `~/.aws/config`, instance metadata)
- Source split across multiple files using [JEP 458](https://openjdk.org/jeps/458); entry point uses implicit `main` via [JEP 512](https://openjdk.org/jeps/512)
- `jpm` `test` action to verify compilation (`javac`)
- `jpm` `clean` action to remove `.class`, `.csv`, and `.zip` artifacts
- Apache 2.0 license

[1.0.0]: https://github.com/JKomoroski/dynamo-to-csv/releases/tag/v1.0.0
