package shared;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ParallelExporter {

    private final ExportConfiguration config;
    private final DynamoDbClient dynamoClient;

    public ParallelExporter(ExportConfiguration config, DynamoDbClient dynamoClient) {
        this.config = config;
        this.dynamoClient = dynamoClient;
    }

    public void export() throws IOException {
        int totalSegments = Runtime.getRuntime().availableProcessors() * 4;
        System.out.println("Starting scan of " + config.tableName());
        System.out.println("Outputting scan to " + config.outputPath());
        if (!config.allAttributes()) {
            System.out.println("Attributes scanned: " + String.join(",", config.attributes()));
        } else {
            System.out.println("Discovering all attributes across " + totalSegments + " segments...");
        }

        List<SegmentExporter.SegmentResult> results = runSegmentsInParallel(totalSegments);

        System.out.println("Joining " + results.size() + " segment files...");
        new AttributeJoiner(results, config).join();
    }

    private List<SegmentExporter.SegmentResult> runSegmentsInParallel(int totalSegments) throws IOException {
        List<Thread> threads = new ArrayList<>(totalSegments);
        List<SegmentExporter.SegmentResult> results = new ArrayList<>(totalSegments);
        List<Throwable> errors = new ArrayList<>();

        for (int i = 0; i < totalSegments; i++) {
            int segment = i;
            var exporter = new SegmentExporter(
                    config.tableName(), segment, totalSegments,
                    config.attributes(), config.allAttributes(), dynamoClient);
            var thread = Thread.ofVirtual().start(() -> {
                try {
                    var result = exporter.export();
                    synchronized (results) {
                        results.add(result);
                    }
                } catch (Throwable t) {
                    synchronized (errors) {
                        errors.add(t);
                    }
                }
            });
            threads.add(thread);
        }

        for (var thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for segment exporters", e);
            }
        }

        if (!errors.isEmpty()) {
            var ex = new IOException("One or more segment exporters failed");
            errors.forEach(ex::addSuppressed);
            throw ex;
        }

        return results;
    }
}

