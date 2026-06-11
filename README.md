# Table Exporter

### Requirements
- [JPM](https://github.com/codejive/java-jpm)
- Java 25

### Usage
0. Export aws creds to your shell environment
1. `jpm install`
2. `./SimpleTableExport.java`
    - Starts in interactive mode
3. `./SimpleTableExport.java [outputFile] [tableName] [attribute1] [attribute2] ...`
    - Runs without interactive mode
### Output
The exporter writes attributes to CSV. DynamoDB types are converted as follows:

| DynamoDB Type | CSV Representation |
|---|---|
| String (`S`) | Raw string value |
| Number (`N`) | Raw number value |
| Boolean (`BOOL`) | `true` / `false` |
| Null (`NUL`) | Empty string |
| List (`L`) | DynamoDB JSON array, `\"` escaped and quoted |
| Map (`M`) | DynamoDB JSON object, `\"` escaped and quoted |
| String Set (`SS`) | JSON string array (e.g. `["a", "b"]`) |
| Number Set (`NS`) | JSON array of number strings (e.g. `["1", "2.5"]`) — strings preserve DynamoDB's arbitrary precision |
| Binary (`B`) | Base64-encoded string |
| Binary Set (`BS`) | JSON array of Base64-encoded strings |

Complex types (`L`, `M`) preserve each element's DynamoDB type wrapper (e.g. `{ "S" : "..." }`), matching the DynamoDB JSON wire format.

### TODO
- Benchmark with a profiler to improve performance, but it's likely limited by dynamo

### References
- JEP 330: Launch Single-File Source-Code Programs
- JEP 458: Launch Multi-File Source-Code Programs
- JEP 477: Instance Main Methods and Implicit Classes
- https://www.youtube.com/watch?v=04wFgshWMdA
- https://horstmann.com/presentations/2025/javaone/#(1)
