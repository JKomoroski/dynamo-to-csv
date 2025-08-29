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
        IO.println("Exporting " + env);
         new SimpleExporter("%s_%s_ax_account".formatted(env, env), Path.of("%s_accounts.csv".formatted(env)),
            new String[]{"partitionId", "accountId"}).scanAccountsToCSV();
         new SimpleExporter("%s_%s_ax_end_user".formatted(env, env), Path.of("%s_end_user.csv".formatted(env)),
                 new String[]{"partitionId", "accountId", "user_id"}).scanAccountsToCSV();
    }
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

    SimpleExporter(String tableName, Path outputPath, String[] projectionExpression) {
        this.tableName = tableName;
        this.outputPath = outputPath;
        this.projectionExpression = projectionExpression;
    }

    void scanAccountsToCSV() throws IOException {
        IO.println("Starting scan of " + tableName);
        try (DynamoDbClient dynamoClient = DynamoDbClient.builder().region(Region.US_EAST_2).build()) {

            ScanRequest scanRequest = ScanRequest.builder()
                    .tableName(tableName)
                    .projectionExpression(String.join(", ", projectionExpression))
                    .build();

            ScanIterable scanIterable = dynamoClient.scanPaginator(scanRequest);

            try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(outputPath))) {
                writer.println(String.join(",", projectionExpression)); //header row

                scanIterable.items()
                        .stream()
                        .map(this::attributesToString)
                        .forEach(s -> writer.printf("%s%n", s));
            }
        }
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

