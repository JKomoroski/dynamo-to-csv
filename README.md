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

### TODO
- Benchmark with a profiler to improve performance, but it's likely limited by dynamo

### References
- JEP 330: Launch Single-File Source-Code Programs
- JEP 458: Launch Multi-File Source-Code Programs
- JEP 477: Instance Main Methods and Implicit Classes
- https://www.youtube.com/watch?v=04wFgshWMdA
- https://horstmann.com/presentations/2025/javaone/#(1)
