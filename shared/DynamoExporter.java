package shared;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import java.io.IOException;

public class DynamoExporter implements AutoCloseable {
    private final DynamoDbClient client;

    public DynamoExporter() {
        this.client = DynamoDbClient.create();
    }

    public void handleCommandLineMode(ExportConfiguration config) throws IOException {
        new ParallelExporter(config, client).export();
    }

    public void handleInteractiveMode() throws Exception {
        var exportConfig = new InteractiveSession(client).configure();
        if (exportConfig != null) {
            new ParallelExporter(exportConfig, client).export();
        }
    }

    @Override
    public void close() {
        client.close();
    }
}

