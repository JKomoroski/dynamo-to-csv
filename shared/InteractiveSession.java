package shared;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class InteractiveSession {

    private final DynamoDbClient dynamoClient;
    private final Scanner scanner;

    InteractiveSession(DynamoDbClient dynamoClient) {
        this.dynamoClient = dynamoClient;
        this.scanner = new Scanner(System.in);
    }

    ExportConfiguration configure() throws Exception {
        System.out.print("Enter output CSV file name: ");
        String outputFile = scanner.nextLine().trim();

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

        return new ExportConfiguration(tableName, Path.of(outputFile),
                selectedAttributes.toArray(new String[0]));
    }

    private String selectTable() throws Exception {
        System.out.println("Listing DynamoDB tables...");
        List<String> tables = dynamoClient.listTables(ListTablesRequest.builder().build()).tableNames();

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

    private Set<String> discoverAttributes(String tableName) {
        System.out.println("Scanning table to discover attributes...");

        ScanRequest scanRequest = ScanRequest.builder()
                .tableName(tableName)
                .limit(100)
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

        System.out.println("\nAvailable attributes:");
        for (int i = 0; i < sortedAttributes.size(); i++) {
            System.out.printf("%d. %s%n", i + 1, sortedAttributes.get(i));
        }

        List<String> selectedAttributes = new ArrayList<>();
        selectFromDiscoveredAttributes(sortedAttributes, selectedAttributes);
        addCustomAttributes(selectedAttributes);

        System.out.println("\nSelected attributes: " + selectedAttributes);
        return selectedAttributes;
    }

    private void selectFromDiscoveredAttributes(List<String> sortedAttributes, List<String> selectedAttributes) {
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
    }

    private void addCustomAttributes(List<String> selectedAttributes) {
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
    }
}
