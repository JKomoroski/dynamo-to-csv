///usr/bin/env java -cp deps/\* --source 25 "$0" "$@" ; exit $?

import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.io.*;
import java.net.URI;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.zip.*;

static final Map<String, String> ENV = Map.of(
        "AWS_ENDPOINT_URL",      "http://localhost:8000",
        "AWS_ACCESS_KEY_ID",     "fakeKey",
        "AWS_SECRET_ACCESS_KEY", "fakeSecret",
        "AWS_REGION",            "us-east-1"
);

static final String TABLE = "ExportIntegrationTest";
static final String PROJECT_DIR = System.getProperty("user.dir");
static DynamoDbClient dynamo;

record RunResult(int exitCode, String output) {}

void main(String[] args) throws Exception {
    dynamo = DynamoDbClient.builder()
            .endpointOverride(URI.create("http://localhost:8000"))
            .region(Region.US_EAST_1)
            .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create("fakeKey", "fakeSecret")))
            .build();

    try {
        seedTable();

        int passed = 0, failed = 0;
        for (var test : List.of(
                "testKnownAttributes",
                "testAllAttributes",
                "testGzip",
                "testZip",
                "testAllAttributesWithGzip",
                "testShortFlags",
                "testDefaultOutputFilename",
                "testInteractiveMode",
                "testInteractiveSpecificAttributes",
                "testInteractiveGzip",
                "testCsvEscaping",
                "testEmptyTable",
                "testHelp",
                "testErrorGzipAndZipTogether",
                "testErrorAttributesAndAllAttributesTogether",
                "testErrorMissingTable",
                "testErrorMissingAttributeSelection",
                "testErrorUnknownFlag"
        )) {
            IO.print("\n[" + test + "] ");
            try {
                switch (test) {
                    case "testKnownAttributes"                    -> testKnownAttributes();
                    case "testAllAttributes"                      -> testAllAttributes();
                    case "testGzip"                               -> testGzip();
                    case "testZip"                                -> testZip();
                    case "testAllAttributesWithGzip"              -> testAllAttributesWithGzip();
                    case "testShortFlags"                         -> testShortFlags();
                    case "testDefaultOutputFilename"              -> testDefaultOutputFilename();
                    case "testInteractiveMode"                    -> testInteractiveMode();
                    case "testInteractiveSpecificAttributes"      -> testInteractiveSpecificAttributes();
                    case "testInteractiveGzip"                    -> testInteractiveGzip();
                    case "testCsvEscaping"                        -> testCsvEscaping();
                    case "testEmptyTable"                         -> testEmptyTable();
                    case "testHelp"                               -> testHelp();
                    case "testErrorGzipAndZipTogether"            -> testErrorGzipAndZipTogether();
                    case "testErrorAttributesAndAllAttributesTogether" -> testErrorAttributesAndAllAttributesTogether();
                    case "testErrorMissingTable"                  -> testErrorMissingTable();
                    case "testErrorMissingAttributeSelection"     -> testErrorMissingAttributeSelection();
                    case "testErrorUnknownFlag"                   -> testErrorUnknownFlag();
                }
                IO.println("PASS");
                passed++;
            } catch (AssertionError | Exception e) {
                IO.println("FAIL — " + e.getMessage());
                failed++;
            }
        }

        IO.println("\n" + (failed == 0 ? "✅" : "❌") + " " + passed + " passed, " + failed + " failed");
        if (failed > 0) System.exit(1);
    } finally {
        dropTable();
        dynamo.close();
    }
}

void seedTable() throws Exception {
    IO.println("Seeding table: " + TABLE);
    try {
        dynamo.deleteTable(DeleteTableRequest.builder().tableName(TABLE).build());
        dynamo.waiter().waitUntilTableNotExists(DescribeTableRequest.builder().tableName(TABLE).build());
    } catch (ResourceNotFoundException ignored) {}

    dynamo.createTable(CreateTableRequest.builder()
            .tableName(TABLE)
            .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
            .attributeDefinitions(AttributeDefinition.builder()
                    .attributeName("id").attributeType(ScalarAttributeType.S).build())
            .billingMode(BillingMode.PAY_PER_REQUEST)
            .build());
    dynamo.waiter().waitUntilTableExists(DescribeTableRequest.builder().tableName(TABLE).build());

    dynamo.putItem(PutItemRequest.builder().tableName(TABLE).item(Map.of(
            "id",     AttributeValue.fromS("1"),
            "name",   AttributeValue.fromS("Alice"),
            "email",  AttributeValue.fromS("alice@example.com"),
            "score",  AttributeValue.fromN("42"),
            "active", AttributeValue.fromBool(true)
    )).build());

    dynamo.putItem(PutItemRequest.builder().tableName(TABLE).item(Map.of(
            "id",    AttributeValue.fromS("2"),
            "name",  AttributeValue.fromS("Bob"),
            "email", AttributeValue.fromS("bob@example.com"),
            "score", AttributeValue.fromN("99"),
            "role",  AttributeValue.fromS("admin")
    )).build());

    dynamo.putItem(PutItemRequest.builder().tableName(TABLE).item(Map.of(
            "id",   AttributeValue.fromS("3"),
            "name", AttributeValue.fromS("Carol, the \"Great\""),
            "tags", AttributeValue.fromL(List.of(
                        AttributeValue.fromS("java"),
                        AttributeValue.fromS("cloud")))
    )).build());

    IO.println("Seeded 3 items");
}

void dropTable() {
    try { dynamo.deleteTable(DeleteTableRequest.builder().tableName(TABLE).build()); } catch (Exception ignored) {}
    try (var stream = Files.list(Path.of(PROJECT_DIR))) {
        stream.filter(p -> {
            String n = p.getFileName().toString();
            return n.startsWith("test_") || n.startsWith(TABLE);
        }).forEach(p -> { try { Files.deleteIfExists(p); } catch (Exception ignored2) {} });
    } catch (Exception ignored) {}
}

void testKnownAttributes() throws Exception {
    Path out = Path.of(PROJECT_DIR, "test_known.csv");
    Files.deleteIfExists(out);

    assertExit(run(List.of("--table", TABLE, "--output", out.toString(), "--attributes", "id,name,email,score")), 0);

    var lines = Files.readAllLines(out);
    assertEqual(lines.get(0), "id,name,email,score", "header");
    assertEqual(lines.size(), 4, "line count");
    assertContains(lines, "alice@example.com", "Alice email");
    assertContains(lines, "42", "Alice score");
    assertContains(lines, "Carol", "Carol row");
}

void testAllAttributes() throws Exception {
    Path out = Path.of(PROJECT_DIR, "test_all.csv");
    Files.deleteIfExists(out);

    assertExit(run(List.of("--table", TABLE, "--output", out.toString(), "--all-attributes")), 0);

    var lines = Files.readAllLines(out);
    String header = lines.get(0);
    for (String attr : List.of("active", "email", "id", "name", "role", "score", "tags"))
        if (!header.contains(attr))
            throw new AssertionError("Missing attr in header: " + attr + " | header=" + header);
    assertEqual(lines.size(), 4, "line count");
    assertContains(lines, "admin", "Bob role");
    assertContains(lines, "Carol", "Carol row");
}

void testGzip() throws Exception {
    Path out = Path.of(PROJECT_DIR, "test_gzip.csv");
    Path expected = Path.of(PROJECT_DIR, "test_gzip.csv.gz");
    Files.deleteIfExists(out);
    Files.deleteIfExists(expected);

    assertExit(run(List.of("--table", TABLE, "--output", out.toString(), "--attributes", "id,name", "--gzip")), 0);
    assertExists(expected);
    assertNotExists(out);

    try (var gis = new GZIPInputStream(Files.newInputStream(expected));
         var reader = new BufferedReader(new InputStreamReader(gis))) {
        assertEqual(reader.readLine(), "id,name", "gzip header");
        assertEqual((int) reader.lines().count(), 3, "gzip row count");
    }
}

void testZip() throws Exception {
    Path out = Path.of(PROJECT_DIR, "test_zip.csv");
    Path expected = Path.of(PROJECT_DIR, "test_zip.csv.zip");
    Files.deleteIfExists(out);
    Files.deleteIfExists(expected);

    assertExit(run(List.of("--table", TABLE, "--output", out.toString(), "--attributes", "id,name", "--zip")), 0);
    assertExists(expected);
    assertNotExists(out);

    try (var zis = new ZipInputStream(Files.newInputStream(expected))) {
        var entry = zis.getNextEntry();
        if (entry == null) throw new AssertionError("Zip has no entries");
        if (!entry.getName().endsWith(".csv")) throw new AssertionError("Zip entry not .csv: " + entry.getName());
        var reader = new BufferedReader(new InputStreamReader(zis));
        assertEqual(reader.readLine(), "id,name", "zip header");
        assertEqual((int) reader.lines().count(), 3, "zip row count");
    }
}

void testDefaultOutputFilename() throws Exception {
    assertExit(run(List.of("--table", TABLE, "--attributes", "id,name")), 0);

    try (var stream = Files.list(Path.of(PROJECT_DIR))) {
        boolean found = stream.anyMatch(p -> {
            String n = p.getFileName().toString();
            return n.startsWith(TABLE + "_") && n.endsWith(".csv");
        });
        if (!found) throw new AssertionError("Default output file not found matching " + TABLE + "_*.csv");
    }
}

void testInteractiveMode() throws Exception {
    String stdin = "1\nall\nn\n\n\n";

    var result = run(List.of(), stdin);
    assertExit(result, 0);

    try (var stream = Files.list(Path.of(PROJECT_DIR))) {
        boolean found = stream.anyMatch(p -> {
            String n = p.getFileName().toString();
            return n.startsWith(TABLE + "_") && n.endsWith(".csv");
        });
        if (!found) throw new AssertionError("Interactive output file not found.\nOutput:\n" + result.output());
    }
}

void testAllAttributesWithGzip() throws Exception {
    Path out = Path.of(PROJECT_DIR, "test_all_gz.csv");
    Path expected = Path.of(PROJECT_DIR, "test_all_gz.csv.gz");
    Files.deleteIfExists(out);
    Files.deleteIfExists(expected);

    assertExit(run(List.of("--table", TABLE, "--output", out.toString(), "--all-attributes", "--gzip")), 0);
    assertExists(expected);
    assertNotExists(out);

    try (var gis = new GZIPInputStream(Files.newInputStream(expected));
         var reader = new BufferedReader(new InputStreamReader(gis))) {
        String header = reader.readLine();
        for (String attr : List.of("active", "email", "id", "name", "role", "score", "tags"))
            if (!header.contains(attr))
                throw new AssertionError("Missing attr in gzip header: " + attr);
        assertEqual((int) reader.lines().count(), 3, "gzip all-attrs row count");
    }
}

void testShortFlags() throws Exception {
    Path out = Path.of(PROJECT_DIR, "test_short.csv.gz");
    Files.deleteIfExists(out);

    assertExit(run(List.of("-t", TABLE, "-o", out.toString(), "-a", "id,name", "-g")), 0);
    assertExists(out);

    try (var gis = new GZIPInputStream(Files.newInputStream(out));
         var reader = new BufferedReader(new InputStreamReader(gis))) {
        assertEqual(reader.readLine(), "id,name", "short flags header");
        assertEqual((int) reader.lines().count(), 3, "short flags row count");
    }
}

void testInteractiveSpecificAttributes() throws Exception {
    // Attributes sorted alphabetically: 1=active 2=email 3=id 4=name 5=role 6=score 7=tags
    // Select id (3) and name (4), then done, no custom attrs, no compression, custom filename
    String out = PROJECT_DIR + "/test_interactive_specific.csv";
    String stdin = "1\n3,4\ndone\nn\n\n" + out + "\n";

    var result = run(List.of(), stdin);
    assertExit(result, 0);

    var lines = Files.readAllLines(Path.of(out));
    assertEqual(lines.get(0), "id,name", "interactive specific attrs header");
    assertEqual(lines.size(), 4, "interactive specific attrs line count");
    assertContains(lines, "Alice", "interactive specific attrs Alice");
}

void testInteractiveGzip() throws Exception {
    // all attributes, gzip compression, default filename
    String stdin = "1\nall\nn\ngzip\n\n";

    var result = run(List.of(), stdin);
    assertExit(result, 0);

    try (var stream = Files.list(Path.of(PROJECT_DIR))) {
        boolean found = stream.anyMatch(p -> {
            String n = p.getFileName().toString();
            return n.startsWith(TABLE + "_") && n.endsWith(".csv.gz");
        });
        if (!found) throw new AssertionError("Interactive gzip file not found.\nOutput:\n" + result.output());
    }
}


void testCsvEscaping() throws Exception {
    Path out = Path.of(PROJECT_DIR, "test_escape.csv");
    Files.deleteIfExists(out);

    assertExit(run(List.of("--table", TABLE, "--output", out.toString(), "--attributes", "id,name")), 0);

    var lines = Files.readAllLines(out);
    boolean carolEscaped = lines.stream().anyMatch(l -> l.contains("\"Carol, the \"\"Great\"\"\""));
    if (!carolEscaped) {
        var carolLine = lines.stream().filter(l -> l.contains("Carol")).findFirst().orElse("<not found>");
        throw new AssertionError("Carol's name not RFC 4180 escaped. Row: " + carolLine);
    }
}

void testEmptyTable() throws Exception {
    String emptyTable = "EmptyIntegrationTest";
    try {
        dynamo.createTable(CreateTableRequest.builder()
                .tableName(emptyTable)
                .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
                .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName("id").attributeType(ScalarAttributeType.S).build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build());
        dynamo.waiter().waitUntilTableExists(DescribeTableRequest.builder().tableName(emptyTable).build());

        Path out = Path.of(PROJECT_DIR, "test_empty.csv");
        Files.deleteIfExists(out);

        assertExit(run(List.of("--table", emptyTable, "--output", out.toString(), "--attributes", "id,name")), 0);

        var lines = Files.readAllLines(out);
        assertEqual(lines.size(), 1, "empty table line count");
        assertEqual(lines.get(0), "id,name", "empty table header");
    } finally {
        try { dynamo.deleteTable(DeleteTableRequest.builder().tableName(emptyTable).build()); } catch (Exception ignored) {}
        Files.deleteIfExists(Path.of(PROJECT_DIR, "test_empty.csv"));
    }
}

void testHelp() throws Exception {
    var result = run(List.of("--help"));
    assertExit(result, 0);
    if (!result.output().contains("--table") || !result.output().contains("--attributes"))
        throw new AssertionError("Help output missing expected flags.\n" + result.output());

    result = run(List.of("-h"));
    assertExit(result, 0);
    if (!result.output().contains("--table"))
        throw new AssertionError("-h help output missing expected flags.\n" + result.output());
}

void testErrorGzipAndZipTogether() throws Exception {
    var result = run(List.of("--table", TABLE, "--attributes", "id", "--gzip", "--zip"));
    if (result.exitCode() == 0) throw new AssertionError("Expected non-zero exit for --gzip + --zip");
    if (!result.output().contains("mutually exclusive"))
        throw new AssertionError("Expected 'mutually exclusive' in output.\n" + result.output());
}

void testErrorAttributesAndAllAttributesTogether() throws Exception {
    var result = run(List.of("--table", TABLE, "--attributes", "id", "--all-attributes"));
    if (result.exitCode() == 0) throw new AssertionError("Expected non-zero exit for --attributes + --all-attributes");
    if (!result.output().contains("mutually exclusive"))
        throw new AssertionError("Expected 'mutually exclusive' in output.\n" + result.output());
}

void testErrorMissingTable() throws Exception {
    var result = run(List.of("--attributes", "id,name"));
    if (result.exitCode() == 0) throw new AssertionError("Expected non-zero exit when --table is missing");
    if (!result.output().contains("--table"))
        throw new AssertionError("Expected '--table' mentioned in error.\n" + result.output());
}

void testErrorMissingAttributeSelection() throws Exception {
    var result = run(List.of("--table", TABLE));
    if (result.exitCode() == 0) throw new AssertionError("Expected non-zero exit when no attributes specified");
    if (!result.output().contains("--attributes"))
        throw new AssertionError("Expected '--attributes' mentioned in error.\n" + result.output());
}

void testErrorUnknownFlag() throws Exception {
    var result = run(List.of("--table", TABLE, "--attributes", "id", "--unknown-flag"));
    if (result.exitCode() == 0) throw new AssertionError("Expected non-zero exit for unknown flag");
    if (!result.output().contains("Unknown flag"))
        throw new AssertionError("Expected 'Unknown flag' in output.\n" + result.output());
}



RunResult run(List<String> args) throws Exception { return run(args, null); }

RunResult run(List<String> extraArgs, String stdin) throws Exception {
    var cmd = new ArrayList<>(List.of("java", "-cp", "deps/*", "--source", "25", "SimpleTableExport.java"));
    cmd.addAll(extraArgs);

    var pb = new ProcessBuilder(cmd).directory(new File(PROJECT_DIR)).redirectErrorStream(true);
    pb.environment().putAll(ENV);

    var process = pb.start();

    if (stdin != null) {
        var stdinBytes = stdin.getBytes();
        CompletableFuture.runAsync(() -> {
            try (var os = process.getOutputStream()) { os.write(stdinBytes); os.flush(); }
            catch (IOException ignored) {}
        });
    } else {
        process.getOutputStream().close();
    }

    String output = new String(process.getInputStream().readAllBytes());
    int exitCode = process.waitFor();
    return new RunResult(exitCode, output);
}

void assertExit(RunResult r, int expected) {
    if (r.exitCode() != expected)
        throw new AssertionError("Expected exit " + expected + " got " + r.exitCode() + "\n" + r.output());
}

void assertEqual(Object actual, Object expected, String label) {
    if (!Objects.equals(actual, expected))
        throw new AssertionError(label + ": expected <" + expected + "> got <" + actual + ">");
}

void assertContains(List<String> lines, String text, String label) {
    if (lines.stream().noneMatch(l -> l.contains(text)))
        throw new AssertionError(label + ": no line contains '" + text + "'");
}

void assertExists(Path path) {
    if (!Files.exists(path)) throw new AssertionError("Expected file: " + path);
}

void assertNotExists(Path path) {
    if (Files.exists(path)) throw new AssertionError("Expected no file: " + path);
}

