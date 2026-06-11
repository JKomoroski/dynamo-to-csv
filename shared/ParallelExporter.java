package shared;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import java.nio.file.Path;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;

public class ParallelExporter {

    private final String tableName;
    private final Path outputPath;
    private final String[] attributes;
    private final DynamoDbClient dynamoClient;
    private final AttributeValueSerializer serializer = new AttributeValueSerializer();

    ParallelExporter(ExportConfiguration configuration, DynamoDbClient dynamoClient) {
        this.tableName = configuration.tableName();
        this.outputPath = configuration.outputPath();
        this.attributes = configuration.attributes();
        this.dynamoClient = dynamoClient;
    }

    void export() {
        var attributeMap = buildAttributeNamesMap();
        String header = String.join(",", attributes);
        String projection = String.join(", ", attributeMap.keySet());

        System.out.println("Starting scan of " + tableName);
        System.out.println("Outputting scan to " + outputPath);
        System.out.println("Attributes scanned " + header);

        try (var csvWriter = new ConcurrentCSVWriter(outputPath)) {
            csvWriter.writeLineAsync(header + "\n");
            scanTableInParallel(projection, attributeMap, csvWriter::writeLineAsync);
        }

        System.out.println("Export completed: " + outputPath);
    }

    private Map<String, String> buildAttributeNamesMap() {
        return Arrays.stream(attributes)
                .collect(Collectors.toMap(s -> "#" + s, Function.identity()));
    }

    private void scanTableInParallel(String projection, Map<String, String> attributeNames, Consumer<String> consumer) {
        int totalSegments = Runtime.getRuntime().availableProcessors() * 4;
        final var builder = ScanRequest.builder().tableName(tableName).totalSegments(totalSegments)
                .projectionExpression(projection).expressionAttributeNames(attributeNames);
        IntStream.range(0, totalSegments)
                .mapToObj(i -> builder.segment(i).build())
                .flatMap(request -> dynamoClient.scanPaginator(request).items().stream())
                .parallel()
                .map(this::itemToCSVLine)
                .forEach(consumer);
    }

    private String itemToCSVLine(Map<String, AttributeValue> item) {
        return Arrays.stream(attributes)
                .map(attr -> item.get(attr))
                .map(value -> value != null ? serializer.serialize(value) : "")
                .map(this::escapeCSV)
                .collect(Collectors.joining(",", "", "\n"));
    }

    private String escapeCSV(String value) {
        if (value == null || value.isEmpty()) return "";
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
        }
        return value;
    }
}
