# Table Exporter

### Requirements
- [JPM](https://github.com/codejive/java-jpm)
- Java 25

### Setup
Export AWS credentials and region to your shell environment (e.g.):
```
export AWS_ACCESS_KEY_ID=""
export AWS_SECRET_ACCESS_KEY=""
export AWS_SESSION_TOKEN=""
export AWS_REGION=""         # or configure via ~/.aws/config
```
or
```
export AWS_PROFILE=""
export AWS_REGION=""
```

Then install dependencies:
```
jpm install
```

### Running

#### Interactive mode
```
./SimpleTableExport.java
```
Guides you through selecting a table, discovering attributes, and choosing output options. Supports large table lists (paginated, filterable) and large attribute sets (paginated, filterable, selectable by number).

#### Non-interactive mode
```
./SimpleTableExport.java --table <name> [--attributes <a1,a2,...> | --all-attributes] [--output <file>] [--gzip | --zip]
```

**Flags:**

| Flag | Short | Description |
|------|-------|-------------|
| `--output <file>` | `-o` | Output file path (default: `<table>_<timestamp>.csv`) |
| `--table <name>` | `-t` | DynamoDB table name (required) |
| `--attributes <a1,a2,...>` | `-a` | Comma-separated attributes to export |
| `--all-attributes` | `-A` | Export all attributes (auto-discovered; see below) |
| `--gzip` | `-g` | Compress output as gzip (appends `.gz` if missing) |
| `--zip` | `-z` | Compress output as zip (appends `.zip` if missing) |
| `--help` | `-h` | Show usage |

**Examples:**
```bash
# Export specific attributes (default output filename)
./SimpleTableExport.java -t MyTable -a id,name,email

# Export all attributes
./SimpleTableExport.java -t MyTable -A

# Gzip compressed, explicit output
./SimpleTableExport.java -t MyTable -a id,name -g -o out.csv.gz

# Zip with all attributes
./SimpleTableExport.java -t MyTable -A -z -o out.zip

# Long-form flags
./SimpleTableExport.java --table MyTable --attributes id,name --output out.csv
```

**Notes:**
- `--output` defaults to `<tableName>_<timestamp>.csv` if not specified
- `--gzip` and `--zip` are mutually exclusive
- `--attributes` and `--all-attributes` are mutually exclusive
- Set `AWS_ENDPOINT_URL` to target a local or custom DynamoDB endpoint (e.g. `http://localhost:8000`)

### Testing

Integration tests require a running DynamoDB local instance on port 8000 (e.g. `docker run -p 8000:8000 amazon/dynamodb-local`).

```bash
jpm run test
# or directly:
java -cp 'deps/*' --source 25 test/IntegrationTest.java
```

The tests seed and tear down their own tables and clean up output files.

### `--all-attributes` implementation notes
When all attributes are unknown upfront, the exporter uses a **single-scan, temp-file-per-segment** approach to avoid a double scan and prevent heap exhaustion:
1. Each parallel segment exports to its own temp file, discovers attributes as items are buffered, and writes a mini-header to its temp file
2. An `AttributeJoiner` merges all temp files: computes the global attribute order, writes the final CSV header, remaps each temp file's columns to the global order, and deletes temp files

Memory per segment is bounded to approximately `totalItems / numSegments`.

### CSV format

Output follows [RFC 4180](https://www.rfc-editor.org/rfc/rfc4180). Fields containing commas, double-quotes, or newlines are wrapped in double-quotes. Embedded double-quotes are escaped by doubling them (`""`):

```
id,name,notes
1,Alice,Normal value
2,Bob,"Value with, comma"
3,Carol,"She said ""hello"""
```

DynamoDB types are serialized as follows:

| DynamoDB Type     | CSV Representation                                   |
|-------------------|------------------------------------------------------|
| String (`S`)      | Raw string value                                     |
| Number (`N`)      | Raw number value                                     |
| Boolean (`BOOL`)  | `true` / `false`                                     |
| Null (`NUL`)      | Empty string                                         |
| List (`L`)        | DynamoDB JSON array (quotes escaped per RFC 4180)    |
| Map (`M`)         | DynamoDB JSON object (quotes escaped per RFC 4180)   |
| String Set (`SS`) | JSON string array (e.g. `["a", "b"]`)                |
| Number Set (`NS`) | JSON array of number strings (e.g. `["1", "2.5"]`)   |
| Binary (`B`)      | Base64-encoded string                                |
| Binary Set (`BS`) | JSON array of Base64-encoded strings                 |

Complex types (`L`, `M`) preserve each element's DynamoDB type wrapper (e.g. `{ "S" : "..." }`), matching the DynamoDB JSON wire format.

### TODO
- Benchmark with a profiler to improve performance, but it's likely limited by DynamoDB

### References
- [JEP 330: Launch Single-File Source-Code Programs](https://openjdk.org/jeps/330)
- [JEP 458: Launch Multi-File Source-Code Programs](https://openjdk.org/jeps/458)
- [JEP 512: Compact Source Files and Instance Main Methods](https://openjdk.org/jeps/512)
- https://www.youtube.com/watch?v=04wFgshWMdA
- https://horstmann.com/presentations/2025/javaone/#(1)
