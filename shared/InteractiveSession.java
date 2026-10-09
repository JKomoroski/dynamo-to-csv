package shared;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class InteractiveSession {

    private static final int PAGE_SIZE = 25;

    private final DynamoDbClient dynamoClient;
    private final Scanner scanner;

    InteractiveSession(DynamoDbClient dynamoClient) {
        this.dynamoClient = dynamoClient;
        this.scanner = new Scanner(System.in);
    }

    ExportConfiguration configure() throws Exception {
        String tableName = selectTable();
        if (tableName == null) {
            System.out.println("No table selected. Exiting.");
            return null;
        }

        Set<String> availableAttributes = discoverAttributes(tableName);
        if (availableAttributes.isEmpty()) {
            System.out.println("No attributes found in table. Exiting.");
            return null;
        }

        List<String> selectedAttributes = selectAttributes(availableAttributes);
        if (selectedAttributes.isEmpty()) {
            System.out.println("No attributes selected. Exiting.");
            return null;
        }

        CompressionType compressionType = promptCompression();
        String outputFile = promptOutputFile(tableName, compressionType);

        return new ExportConfiguration(tableName, Path.of(outputFile),
                selectedAttributes.toArray(new String[0]), false, compressionType);
    }

    private CompressionType promptCompression() {
        while (true) {
            System.out.print("Compress output? [none/gzip/zip] (default: none): ");
            String input = scanner.nextLine().trim().toLowerCase();
            switch (input) {
                case "", "none" -> { return CompressionType.NONE; }
                case "gzip"     -> { return CompressionType.GZIP; }
                case "zip"      -> { return CompressionType.ZIP; }
                default -> System.out.println("Please enter none, gzip, or zip.");
            }
        }
    }

    private String promptOutputFile(String tableName, CompressionType compressionType) {
        String extension = switch (compressionType) {
            case GZIP -> ".csv.gz";
            case ZIP  -> ".zip";
            case NONE -> ".csv";
        };
        String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        String defaultName = tableName + "_" + ts + extension;

        System.out.printf("Enter output file name (press Enter for default: %s): ", defaultName);
        String input = scanner.nextLine().trim();
        return input.isEmpty() ? defaultName : input;
    }

    private String selectTable() {
        System.out.println("Listing DynamoDB tables...");
        List<String> allTables = fetchAllTables();

        if (allTables.isEmpty()) {
            System.out.println("No tables found.");
            return null;
        }

        System.out.printf("Found %d table(s).%n", allTables.size());

        List<String> filtered = promptTableFilter(allTables);
        if (filtered.isEmpty()) {
            System.out.println("No tables match that filter.");
            return null;
        }

        return pickFromPagedList(filtered, "table");
    }

    private List<String> fetchAllTables() {
        List<String> tables = new ArrayList<>();
        String lastKey = null;
        do {
            var req = ListTablesRequest.builder().exclusiveStartTableName(lastKey).build();
            var resp = dynamoClient.listTables(req);
            tables.addAll(resp.tableNames());
            lastKey = resp.lastEvaluatedTableName();
        } while (lastKey != null);
        return tables;
    }

    private List<String> promptTableFilter(List<String> tables) {
        if (tables.size() <= PAGE_SIZE) return tables;
        System.out.print("Filter tables by name substring (press Enter for all): ");
        String filter = scanner.nextLine().trim();
        if (filter.isEmpty()) return tables;
        return tables.stream()
                .filter(t -> t.contains(filter))
                .collect(Collectors.toList());
    }

    private String pickFromPagedList(List<String> items, String label) {
        int page = 0;
        while (true) {
            int start = page * PAGE_SIZE;
            int end = Math.min(start + PAGE_SIZE, items.size());
            System.out.printf("%nShowing %s %d-%d of %d:%n", label + "s", start + 1, end, items.size());
            for (int i = start; i < end; i++) {
                System.out.printf("%d. %s%n", i + 1, items.get(i));
            }

            boolean hasNext = end < items.size();
            boolean hasPrev = page > 0;
            System.out.printf("%nSelect %s (number)%s%s: ",
                    label,
                    hasNext ? ", 'next' for more" : "",
                    hasPrev ? ", 'prev' for previous" : "");
            String input = scanner.nextLine().trim();

            switch (input.toLowerCase()) {
                case "next" -> { if (hasNext) page++; else System.out.println("No more pages."); }
                case "prev" -> { if (hasPrev) page--; else System.out.println("Already on first page."); }
                default -> {
                    try {
                        int selection = Integer.parseInt(input);
                        if (selection >= 1 && selection <= items.size()) {
                            return items.get(selection - 1);
                        }
                        System.out.println("Invalid selection. Enter a number between 1 and " + items.size());
                    } catch (NumberFormatException e) {
                        System.out.println("Please enter a valid number or navigation command.");
                    }
                }
            }
        }
    }

    private Set<String> discoverAttributes(String tableName) {
        System.out.println("Scanning table to discover attributes (sample of 100 items)...");

        ScanRequest scanRequest = ScanRequest.builder()
                .tableName(tableName)
                .limit(100)
                .build();

        var attributes = dynamoClient.scan(scanRequest).items()
                .stream()
                .flatMap(item -> item.keySet().stream())
                .collect(Collectors.toSet());

        System.out.printf("Found %d unique attributes from sample data.%n", attributes.size());
        return attributes;
    }

    private List<String> selectAttributes(Set<String> availableAttributes) {
        List<String> sortedAttributes = new ArrayList<>(availableAttributes);
        Collections.sort(sortedAttributes);

        List<String> selectedAttributes = new ArrayList<>();
        selectFromDiscoveredAttributes(sortedAttributes, selectedAttributes);
        addCustomAttributes(selectedAttributes);

        System.out.println("\nSelected attributes: " + selectedAttributes);
        return selectedAttributes;
    }

    private void selectFromDiscoveredAttributes(List<String> sortedAttributes, List<String> selectedAttributes) {
        int page = 0;
        String filterSubstring = null;

        while (true) {
            final String currentFilter = filterSubstring;
            List<String> visible = currentFilter == null ? sortedAttributes
                    : sortedAttributes.stream().filter(a -> a.contains(currentFilter)).collect(Collectors.toList());

            int start = page * PAGE_SIZE;
            if (start >= visible.size()) { page = 0; start = 0; }
            int end = Math.min(start + PAGE_SIZE, visible.size());
            System.out.printf("%nAvailable attributes (%d-%d of %d%s):%n",
                    start + 1, end, visible.size(),
                    filterSubstring != null ? ", filter: '" + filterSubstring + "'" : "");
            for (int i = start; i < end; i++) {
                System.out.printf("%d. %s%n", i + 1, visible.get(i));
            }

            boolean hasNext = end < visible.size();
            boolean hasPrev = page > 0;
            System.out.println("\nCommands: enter number(s) separated by commas, 'all'" +
                    (hasNext ? ", 'next'" : "") + (hasPrev ? ", 'prev'" : "") +
                    ", 'filter <substring>', 'list', 'done'");
            System.out.print("> ");
            String input = scanner.nextLine().trim();

            if (input.equalsIgnoreCase("done")) break;
            if (input.equalsIgnoreCase("all")) {
                for (String a : sortedAttributes) {
                    if (!selectedAttributes.contains(a)) selectedAttributes.add(a);
                }
                System.out.println("Selected all " + sortedAttributes.size() + " attributes.");
                break;
            }
            if (input.equalsIgnoreCase("next")) {
                if (hasNext) page++;
                else System.out.println("No more pages.");
                continue;
            }
            if (input.equalsIgnoreCase("prev")) {
                if (hasPrev) page--;
                else System.out.println("Already on first page.");
                continue;
            }
            if (input.equalsIgnoreCase("list")) {
                page = 0;
                continue;
            }
            if (input.toLowerCase().startsWith("filter ")) {
                filterSubstring = input.substring(7).trim();
                if (filterSubstring.isEmpty()) filterSubstring = null;
                page = 0;
                continue;
            }

            try {
                for (String part : input.split(",")) {
                    int index = Integer.parseInt(part.trim());
                    if (index >= 1 && index <= visible.size()) {
                        String attribute = visible.get(index - 1);
                        if (!selectedAttributes.contains(attribute)) {
                            selectedAttributes.add(attribute);
                            System.out.println("Added: " + attribute);
                        }
                    } else {
                        System.out.println("Invalid selection: " + index);
                    }
                }
            } catch (NumberFormatException e) {
                System.out.println("Unknown command. Enter numbers, 'all', 'next', 'filter <text>', 'list', or 'done'.");
            }
        }
    }

    private void addCustomAttributes(List<String> selectedAttributes) {
        System.out.print("\nAdd any additional attributes not in the sample? (y/n): ");
        if (scanner.nextLine().trim().toLowerCase().startsWith("y")) {
            while (true) {
                System.out.print("Enter attribute name (or 'done' to finish): ");
                String attribute = scanner.nextLine().trim();
                if ("done".equalsIgnoreCase(attribute)) break;
                if (!attribute.isEmpty() && !selectedAttributes.contains(attribute)) {
                    selectedAttributes.add(attribute);
                    System.out.println("Added: " + attribute);
                }
            }
        }
    }
}

