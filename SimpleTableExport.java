///usr/bin/env java -cp deps/\* --source 25 "$0" "$@" ; exit $?

import shared.DynamoExporter;

void main(String[] args) throws Exception {

    if (args != null && args.length > 0 && args.length < 3) {
        IO.println(
                "Non-Interactive Usage: ./SimpleTableExport.java [outputFile] [tableName] [attribute1] [attribute2] ...");
        IO.println("Interactive Usage: ./SimpleTableExport.java");
        return;
    }

    try (var exporter = new DynamoExporter(args)) {
        if (args != null && args.length >= 3) {
            exporter.handleCommandLineMode();
        } else {
            exporter.handleInteractiveMode();
        }
    }
}
