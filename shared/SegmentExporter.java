package shared;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class SegmentExporter {

    public record SegmentResult(Path tempFile, List<String> attributes) {}

    private final String tableName;
    private final int segment;
    private final int totalSegments;
    private final String[] knownAttributes;
    private final boolean allAttributes;
    private final DynamoDbClient dynamoClient;
    private final AttributeValueSerializer serializer = new AttributeValueSerializer();

    SegmentExporter(String tableName, int segment, int totalSegments,
                    String[] knownAttributes, boolean allAttributes,
                    DynamoDbClient dynamoClient) {
        this.tableName = tableName;
        this.segment = segment;
        this.totalSegments = totalSegments;
        this.knownAttributes = knownAttributes;
        this.allAttributes = allAttributes;
        this.dynamoClient = dynamoClient;
    }

    SegmentResult export() throws IOException {
        Path tempFile = Files.createTempFile("dynamo-segment-" + segment + "-", ".csv");
        if (allAttributes) {
            return exportAllAttributes(tempFile);
        } else {
            return exportKnownAttributes(tempFile);
        }
    }

    private SegmentResult exportKnownAttributes(Path tempFile) throws IOException {
        List<String> attrs = Arrays.asList(knownAttributes);
        Map<String, String> attributeNamesMap = buildAttributeNamesMap(attrs);
        String projection = String.join(", ", attributeNamesMap.keySet());

        try (var writer = Files.newBufferedWriter(tempFile)) {
            writer.write(String.join(",", attrs) + "\n");
            var builder = ScanRequest.builder()
                    .tableName(tableName)
                    .totalSegments(totalSegments)
                    .segment(segment)
                    .projectionExpression(projection)
                    .expressionAttributeNames(attributeNamesMap);
            for (var item : dynamoClient.scanPaginator(builder.build()).items()) {
                writer.write(itemToCSVLine(item, attrs));
            }
        }
        return new SegmentResult(tempFile, attrs);
    }

    private SegmentResult exportAllAttributes(Path tempFile) throws IOException {
        List<Map<String, AttributeValue>> buffer = new ArrayList<>();
        TreeSet<String> discoveredAttributes = new TreeSet<>();

        var builder = ScanRequest.builder()
                .tableName(tableName)
                .totalSegments(totalSegments)
                .segment(segment);
        for (var item : dynamoClient.scanPaginator(builder.build()).items()) {
            buffer.add(item);
            discoveredAttributes.addAll(item.keySet());
        }

        List<String> attrs = new ArrayList<>(discoveredAttributes);
        try (var writer = Files.newBufferedWriter(tempFile)) {
            writer.write(String.join(",", attrs) + "\n");
            for (var item : buffer) {
                writer.write(itemToCSVLine(item, attrs));
            }
        }
        return new SegmentResult(tempFile, attrs);
    }

    private Map<String, String> buildAttributeNamesMap(List<String> attributes) {
        return attributes.stream()
                .collect(Collectors.toMap(s -> "#" + s, s -> s));
    }

    private String itemToCSVLine(Map<String, AttributeValue> item, List<String> attributes) {
        return attributes.stream()
                .map(attr -> {
                    var val = item.get(attr);
                    return val != null ? serializer.serialize(val) : "";
                })
                .map(this::escapeCSV)
                .collect(Collectors.joining(",", "", "\n"));
    }

    private String escapeCSV(String value) {
        if (value == null || value.isEmpty()) return "";
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }
}
