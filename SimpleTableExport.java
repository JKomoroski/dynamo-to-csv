///usr/bin/env java -cp deps/\* --source 25 "$0" "$@" ; exit $?

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.paginators.ScanIterable;

// Runs 2 exports for each environment
void main() throws Exception {
    final List<String> envs =
//                List.of("prod");
            List.of("dev", "qa", "beta");

    for (var env : envs) {
        System.out.printf("Exporting %s%n", env);
        new SimpleExporter("%s_%s_ax_account".formatted(env, env),
                Path.of("%s_accounts.csv".formatted(env)),
                new String[]{"partitionId", "accountId"}
        ).scanAccountsToCSV();
        new SimpleExporter("%s_%s_ax_end_user".formatted(env, env),
                Path.of("%s_end_user.csv".formatted(env)),
                new String[]{"partitionId", "accountId", "user_id"}
        ).scanAccountsToCSV();
    }
}

class SimpleExporter {

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

    SimpleExporter(String tableName, Path outputPath, String[] projectionExpression) {
        this.tableName = tableName;
        this.outputPath = outputPath;
        this.projectionExpression = projectionExpression;
    }

    void scanAccountsToCSV() throws IOException {
        try (DynamoDbClient dynamoClient = DynamoDbClient.builder().region(Region.US_EAST_2).build()) {

            ScanRequest scanRequest = ScanRequest.builder()
                    .tableName(tableName)
                    .projectionExpression(String.join(", ", projectionExpression))
                    .build();

            ScanIterable scanIterable = dynamoClient.scanPaginator(scanRequest);

            try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(outputPath))) {
                writer.println(String.join(",", projectionExpression));

                scanIterable.items().stream()
                        .forEach(item -> {

                            var row = new ArrayList<String>();

                            for (var e : projectionExpression) {
                                var raw = getAttributeValue(item, e);
                                var escaped = escapeCSV(raw);
                                row.add(escaped);
                            }

                            writer.printf("%s%n", String.join(",", row));
                        });
            }
        }
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

