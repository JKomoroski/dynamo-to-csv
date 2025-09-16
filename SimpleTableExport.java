///usr/bin/env java -cp deps/\* --source 25 "$0" "$@" ; exit $?

import module java.base;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.paginators.ScanIterable;

public class SimpleTableExport {

    private static DynamoDbClient CLIENT;

    void main(String[] args) throws Exception {

        if (args != null && args.length <= 2) {
            System.out.println(
                    "Non-Interactive Usage: ./SimpleTableExport.java [outputFile] [tableName] [attribute1] [attribute2]");
            System.out.println("Interactive Usage: ./SimpleTableExport.java");
        }

        try (var c = DynamoDbClient.builder().region(Region.US_EAST_2).build()) {
            CLIENT = c;
            if (args != null && args.length >= 3) {
                // Command line mode: outputFile tableName attribute1 attribute2 ...
                String outputFile = args[0];
                String tableName = args[1];
                String[] attributes = Arrays.copyOfRange(args, 2, args.length);

                new SimpleExporter(tableName, Path.of(outputFile), attributes, CLIENT).scanToCSV();
                return;
            }

            // Interactive mode
            Scanner scanner = new Scanner(System.in);

            // Get output file
            System.out.print("Enter output CSV file name: ");
            String outputFile = scanner.nextLine().trim();

            // List tables and let user select
            String tableName = selectTable(scanner);
            if (tableName == null) {
                System.out.println("No table selected. Exiting.");
                return;
            }

            // Get attributes from a sample of the table
            Set<String> availableAttributes = discoverAttributes(tableName);
            if (availableAttributes.isEmpty()) {
                System.out.println("No attributes found in table. Exiting.");
                return;
            }

            // Let user select attributes
            List<String> selectedAttributes = selectAttributes(scanner, availableAttributes);
            if (selectedAttributes.isEmpty()) {
                System.out.println("No attributes selected. Exiting.");
                return;
            }

            // Build and run the exporter
            new SimpleExporter(tableName, Path.of(outputFile), selectedAttributes.toArray(new String[0]), CLIENT).scanToCSV();
        }
    }

    static String selectTable(Scanner scanner) throws Exception {

        System.out.println("Listing DynamoDB tables...");
        List<String> tables = CLIENT.listTables(ListTablesRequest.builder().build()).tableNames();

        if (tables.isEmpty()) {
            System.out.println("No tables found.");
            return null;
        }

        System.out.println("\nAvailable tables:");
        for (int i = 0; i < tables.size(); i++) {
            System.out.printf("%d. %s%n", i + 1, tables.get(i));
        }

        while (true) {
            System.out.print("\nSelect table (enter number): ");
            String input = scanner.nextLine().trim();

            try {
                int selection = Integer.parseInt(input);
                if (selection >= 1 && selection <= tables.size()) {
                    return tables.get(selection - 1);
                } else {
                    System.out.println("Invalid selection. Please enter a number between 1 and " + tables.size());
                }
            } catch (NumberFormatException e) {
                System.out.println("Please enter a valid number.");
            }
        }
    }

    static Set<String> discoverAttributes(String tableName) {
        Set<String> attributes = new HashSet<>();
        System.out.println("Scanning table to discover attributes...");

        ScanRequest scanRequest = ScanRequest.builder()
                .tableName(tableName)
                .limit(100) // Just scan first 100 items to discover attributes
                .build();

        var response = CLIENT.scan(scanRequest);

        for (var item : response.items()) {
            attributes.addAll(item.keySet());
        }

        System.out.printf("Found %d unique attributes from sample data.%n", attributes.size());

        return attributes;
    }

    static List<String> selectAttributes(Scanner scanner, Set<String> availableAttributes) {
        List<String> sortedAttributes = new ArrayList<>(availableAttributes);
        Collections.sort(sortedAttributes);

        System.out.println("\nAvailable attributes:");
        for (int i = 0; i < sortedAttributes.size(); i++) {
            System.out.printf("%d. %s%n", i + 1, sortedAttributes.get(i));
        }

        List<String> selectedAttributes = new ArrayList<>();

        while (true) {
            System.out.print("\nSelect attributes (enter numbers separated by commas, or 'done' to finish): ");
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
                            System.out.println("Added: " + attribute);
                        }
                    } else {
                        System.out.println("Invalid selection: " + index);
                    }
                }
            } catch (NumberFormatException e) {
                System.out.println("Please enter valid numbers separated by commas.");
            }
        }

        // Ask if user wants to add additional attributes
        System.out.print("\nWould you like to add any additional attributes not found in the sample? (y/n): ");
        if (scanner.nextLine().trim().toLowerCase().startsWith("y")) {
            while (true) {
                System.out.print("Enter attribute name (or 'done' to finish): ");
                String attribute = scanner.nextLine().trim();
                if ("done".equalsIgnoreCase(attribute)) {
                    break;
                }
                if (!attribute.isEmpty() && !selectedAttributes.contains(attribute)) {
                    selectedAttributes.add(attribute);
                    System.out.println("Added: " + attribute);
                }
            }
        }

        System.out.println("\nSelected attributes: " + selectedAttributes);
        return selectedAttributes;
    }

    static class SimpleExporter {

        /**
         * name of the dynamo table to scan
         */
        private final String tableName;
        /**
         * Path to the output csv file
         */
        private final Path outputPath;
        /**
         * List of attributes to get from dynamo, they will also be used as the output csv column names
         */
        private final String[] projectionExpression;

        private final DynamoDbClient dynamoClient;

        SimpleExporter(String tableName, Path outputPath, String[] projectionExpression, DynamoDbClient dynamoClient) {
            this.tableName = tableName;
            this.outputPath = outputPath;
            this.projectionExpression = projectionExpression;
            this.dynamoClient = dynamoClient;
        }

        void scanToCSV() throws IOException {
            IO.println("Starting scan of " + tableName);

            var queue = new ArrayBlockingQueue<String>(1000);
            AtomicBoolean done = new AtomicBoolean(false);
            var executor = Executors.newSingleThreadScheduledExecutor();
            CompletableFuture<Void> writer = CompletableFuture.runAsync(() -> {
                try (BufferedWriter bw = Files.newBufferedWriter(outputPath, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                    while (true) {
                        String line = queue.poll(100, TimeUnit.MILLISECONDS);
                        if (line != null) {
                            bw.write(line);
                        } else if (done.get() && queue.isEmpty()) {
                            break;
                        }
                    }
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }, executor);


            int totalSegments = Runtime.getRuntime().availableProcessors(); // or whatever number you prefer
            IntStream.range(0, totalSegments)
                    .mapToObj(i -> ScanRequest.builder().tableName(tableName).totalSegments(totalSegments).segment(i).build())
                    .map(r -> dynamoClient.scanPaginator(r))
                    .flatMap(iter -> iter.items().stream())
                    .parallel()
                    .map(this::attributesToString)
                    .map(line -> line + "\n")
                    .forEach(l -> {
                        try {
                            queue.put(l);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    });
            done.set(true);
            writer.join();
            executor.shutdown();
            System.out.println("Export completed: " + outputPath);
        }

        private String attributesToString(Map<String, AttributeValue> item) {
            return Arrays.stream(projectionExpression)
                    .map(e -> getAttributeValue(item, e))
                    .map(this::escapeCSV)
                    .collect(Collectors.joining(","));
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
}
