///usr/bin/env java -cp deps/\* --source 25 "$0" "$@" ; exit $?

import shared.CompressionType;
import shared.DynamoExporter;
import shared.ExportConfiguration;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

void main(String[] args) throws Exception {
    if (args == null || args.length == 0) {
        try (var exporter = new DynamoExporter()) {
            exporter.handleInteractiveMode();
        }
        return;
    }

    String outputFile = null;
    String tableName = null;
    List<String> attributes = new ArrayList<>();
    boolean allAttributes = false;
    boolean gzip = false;
    boolean zip = false;

    for (int i = 0; i < args.length; i++) {
        switch (args[i]) {
            case "--help", "-h" -> {
                printHelp();
                return;
            }
            case "--output", "-o" -> {
                if (++i >= args.length) { error("--output requires a value"); return; }
                outputFile = args[i];
            }
            case "--table", "-t" -> {
                if (++i >= args.length) { error("--table requires a value"); return; }
                tableName = args[i];
            }
            case "--attributes", "-a" -> {
                if (++i >= args.length) { error("--attributes requires a value"); return; }
                attributes.addAll(List.of(args[i].split(",")));
            }
            case "--all-attributes", "-A" -> allAttributes = true;
            case "--gzip", "-g" -> gzip = true;
            case "--zip", "-z" -> zip = true;
            default -> { error("Unknown flag: " + args[i]); return; }
        }
    }

    if (gzip && zip) {
        error("--gzip and --zip are mutually exclusive");
        return;
    }

    CompressionType compressionType = gzip ? CompressionType.GZIP : zip ? CompressionType.ZIP : CompressionType.NONE;

    if (!attributes.isEmpty() && allAttributes) {
        error("--attributes and --all-attributes are mutually exclusive");
        return;
    }

    if (outputFile == null || tableName == null) {
        if (tableName == null) {
            error("--table is required in non-interactive mode");
            printHelp();
            return;
        }
        // Default output: <tableName>_<timestamp>.csv
        String ts = java.time.LocalDateTime.now().format(
                java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        outputFile = tableName + "_" + ts + ".csv";
        IO.println("No --output specified, defaulting to: " + outputFile);
    }

    if (attributes.isEmpty() && !allAttributes) {
        error("--attributes or --all-attributes is required in non-interactive mode");
        printHelp();
        return;
    }

    var config = new ExportConfiguration(
            tableName,
            Path.of(outputFile),
            attributes.toArray(new String[0]),
            allAttributes,
            compressionType
    );

    try (var exporter = new DynamoExporter()) {
        exporter.handleCommandLineMode(config);
    }
}

void printHelp() {
    IO.println("""
            dynamo-to-csv — Export DynamoDB tables to CSV
            
            Usage:
              ./SimpleTableExport.java                              Interactive mode
              ./SimpleTableExport.java [flags]                      Non-interactive mode
            
            Flags:
              --output,         -o <file>     Output file path (default: <table>_<timestamp>.csv)
              --table,          -t <name>     DynamoDB table name (required)
              --attributes,     -a <a1,a2,..> Comma-separated attribute names to export
              --all-attributes, -A            Export all attributes (single-scan, temp-file approach)
              --gzip,           -g            Compress output as gzip (appends .gz if missing)
              --zip,            -z            Compress output as zip (appends .zip if missing)
              --help,           -h            Show this help
            
            Examples:
              ./SimpleTableExport.java -t MyTable -a id,name,email
              ./SimpleTableExport.java -t MyTable -A
              ./SimpleTableExport.java -t MyTable -a id,name -g
              ./SimpleTableExport.java -t MyTable -A -z -o out.zip
              ./SimpleTableExport.java --table MyTable --attributes id,name --output out.csv
            
            Notes:
              --output defaults to <tableName>_<timestamp>.csv if not specified
              --gzip and --zip are mutually exclusive
              --attributes and --all-attributes are mutually exclusive
              AWS credentials are resolved via the standard provider chain
              (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, ~/.aws/config, etc.)
            """);
}

void error(String message) {
    IO.println("Error: " + message);
    System.exit(1);
}

