///usr/bin/env java -cp deps/\* --source 25 "$0" "$@" ; exit $?

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

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

static class DynamoExporter implements AutoCloseable {
    private final String[] args;
    private final DynamoDbClient client;

    DynamoExporter(String[] args) {
        this.args = args;
        this.client = DynamoDbClient.builder().region(Region.US_EAST_2).build();
    }

    void handleCommandLineMode() throws IOException {
        var config = new ExportConfiguration(
                args[1],
                Path.of(args[0]),
                Arrays.copyOfRange(args, 2, args.length)
        );
        new ParallelExporter(config, client).export();
    }

    void handleInteractiveMode() throws Exception {
        var exportConfig = new InteractiveSession(client).configure();
        new ParallelExporter(exportConfig, client).export();
    }

    @Override
    public void close() {
        client.close();
    }
}

record ExportConfiguration(String tableName, Path outputPath, String[] attributes) {}

static class InteractiveSession {

    private final DynamoDbClient dynamoClient;
    private final Scanner scanner;

    InteractiveSession(DynamoDbClient dynamoClient) {
        this.dynamoClient = dynamoClient;
        this.scanner = new Scanner(System.in);
    }

    ExportConfiguration configure() throws Exception {
        // Get output file
        IO.print("Enter output CSV file name: ");
        String outputFile = scanner.nextLine().trim();

        // Select table
        String tableName = selectTable();
        if (tableName == null) {
            IO.println("No table selected. Exiting.");
            return null;
        }

        // Discover and select attributes
        Set<String> availableAttributes = discoverAttributes(tableName);
        if (availableAttributes.isEmpty()) {
            IO.println("No attributes found in table. Exiting.");
            return null;
        }

        List<String> selectedAttributes = selectAttributes(availableAttributes);
        if (selectedAttributes.isEmpty()) {
            IO.println("No attributes selected. Exiting.");
            return null;
        }

        return new ExportConfiguration(tableName, Path.of(outputFile),
                selectedAttributes.toArray(new String[0]));
    }

    private String selectTable() throws Exception {
        IO.println("Listing DynamoDB tables...");
        List<String> tables = dynamoClient.listTables(ListTablesRequest.builder().build()).tableNames();

        if (tables.isEmpty()) {
            IO.println("No tables found.");
            return null;
        }

        IO.println("\nAvailable tables:");
        for (int i = 0; i < tables.size(); i++) {
            System.out.printf("%d. %s%n", i + 1, tables.get(i));
        }

        while (true) {
            IO.print("\nSelect table (enter number): ");
            String input = scanner.nextLine().trim();

            try {
                int selection = Integer.parseInt(input);
                if (selection >= 1 && selection <= tables.size()) {
                    return tables.get(selection - 1);
                } else {
                    IO.println("Invalid selection. Please enter a number between 1 and " + tables.size());
                }
            } catch (NumberFormatException e) {
                IO.println("Please enter a valid number.");
            }
        }
    }

    private Set<String> discoverAttributes(String tableName) {
        IO.println("Scanning table to discover attributes...");

        ScanRequest scanRequest = ScanRequest.builder()
                .tableName(tableName)
                .limit(100) // Just scan first 100 items to discover attributes
                .build();

        var attributes = dynamoClient.scan(scanRequest).items()
                .stream()
                .flatMap(item -> item.keySet().stream())
                .collect(Collectors.toSet());

        System.out.printf("Found %d unique attributes from sample data. %n", attributes.size());
        return attributes;
    }

    private List<String> selectAttributes(Set<String> availableAttributes) {
        List<String> sortedAttributes = new ArrayList<>(availableAttributes);
        Collections.sort(sortedAttributes);

        IO.println("\nAvailable attributes:");
        for (int i = 0; i < sortedAttributes.size(); i++) {
            System.out.printf("%d. %s%n", i + 1, sortedAttributes.get(i));
        }

        List<String> selectedAttributes = new ArrayList<>();
        selectFromDiscoveredAttributes(sortedAttributes, selectedAttributes);
        addCustomAttributes(selectedAttributes);

        IO.println("\nSelected attributes: " + selectedAttributes);
        return selectedAttributes;
    }

    private void selectFromDiscoveredAttributes(List<String> sortedAttributes, List<String> selectedAttributes) {
        while (true) {
            IO.print("\nSelect attributes (enter numbers separated by commas, or 'done' to finish): ");
            String input = scanner.nextLine().trim();

            if ("done".equalsIgnoreCase(input)) {
                break;
            }

            try {
                String[] selections = input.split(",");
                for (String selection : selections) {
                    int index = Integer.parseInt(selection.trim());
                    if (index >= 1 && index <= sortedAttributes.size()) {
                        String attribute = sortedAttributes.get(index - 1);
                        if (!selectedAttributes.contains(attribute)) {
                            selectedAttributes.add(attribute);
                            IO.println("Added: " + attribute);
                        }
                    } else {
                        IO.println("Invalid selection: " + index);
                    }
                }
            } catch (NumberFormatException e) {
                IO.println("Please enter valid numbers separated by commas.");
            }
        }
    }

    private void addCustomAttributes(List<String> selectedAttributes) {
        IO.print("\nWould you like to add any additional attributes not found in the sample? (y/n): ");
        if (scanner.nextLine().trim().toLowerCase().startsWith("y")) {
            while (true) {
                IO.print("Enter attribute name (or 'done' to finish): ");
                String attribute = scanner.nextLine().trim();
                if ("done".equalsIgnoreCase(attribute)) {
                    break;
                }
                if (!attribute.isEmpty() && !selectedAttributes.contains(attribute)) {
                    selectedAttributes.add(attribute);
                    IO.println("Added: " + attribute);
                }
            }
        }
    }
}

static class ParallelExporter {

    private final String tableName;
    private final Path outputPath;
    private final String[] attributes;
    private final DynamoDbClient dynamoClient;

    ParallelExporter(ExportConfiguration configuration, DynamoDbClient dynamoClient) {
        this.tableName = configuration.tableName();
        this.outputPath = configuration.outputPath();
        this.attributes = configuration.attributes();
        this.dynamoClient = dynamoClient;
    }

    void export() {
        var attributeMap = buildAttributeNamesMap();
        String header = String.join(",", attributeMap.values());
        String projection = String.join(", ", attributeMap.keySet());

        IO.println("Starting scan of " + tableName);
        IO.println("Outputting scan to " + outputPath);
        IO.println("Attributes scanned " + header);

        try (var csvWriter = new ConcurrentCSVWriter(outputPath)) {
            csvWriter.writeLineAsync(header + "\n");
            scanTableInParallel(projection, attributeMap, csvWriter::writeLineAsync);
        }

        IO.println("Export completed: " + outputPath);
    }

    private Map<String, String> buildAttributeNamesMap() {
        return Arrays.stream(attributes)
                .collect(Collectors.toMap(s -> "#" + s, Function.identity()));
    }

    private void scanTableInParallel(String projection, Map<String, String> attributeNames, Consumer<String> consumer) {
        int totalSegments = Runtime.getRuntime().availableProcessors() * 4;
        final var builder = ScanRequest.builder().tableName(tableName).totalSegments(totalSegments).projectionExpression(projection)
                .expressionAttributeNames(attributeNames);
        IntStream.range(0, totalSegments)
                .mapToObj(i -> builder.segment(i).build())
                .flatMap(request -> dynamoClient.scanPaginator(request).items().stream())
                .parallel()
                .map(this::itemToCSVLine)
                .forEach(consumer);
    }

    private String itemToCSVLine(Map<String, AttributeValue> item) {
        return Arrays.stream(attributes)
                .map(attr -> getAttributeValue(item, attr))
                .map(this::escapeCSV)
                .collect(Collectors.joining(",", "", "\n"));
    }

    private String getAttributeValue(Map<String, AttributeValue> item, String key) {
        AttributeValue value = item.get(key);
        return value != null ? value.s() : "";
    }

    private String escapeCSV(String value) {
        if (value == null) {
            return "";
        }
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }
}

static class ConcurrentCSVWriter implements AutoCloseable {

    private final ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(10_000);
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final AtomicBoolean done = new AtomicBoolean(false);
    private final CompletableFuture<Void> writerTask;

    ConcurrentCSVWriter(Path outputPath) {
        this.writerTask = CompletableFuture.runAsync(() -> {
            try (var writer = Files.newBufferedWriter(outputPath)) {
                do {
                    String line = queue.poll(1, TimeUnit.SECONDS);
                    if (line != null) {
                        writer.write(line);
                    }
                } while (!done.get() || !queue.isEmpty());
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, executor);
    }

    void writeLineAsync(String line) {
        try {
            queue.put(line);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            done.set(true);
            writerTask.join();
        } finally {
            executor.shutdown();
        }
    }
}
