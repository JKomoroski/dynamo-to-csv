package shared;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

public class DynamoExporter implements AutoCloseable {
    private final String[] args;
    private final DynamoDbClient client;

    public DynamoExporter(String[] args) {
        this.args = args;
        // Region is resolved via the standard AWS provider chain:
        // AWS_REGION / AWS_DEFAULT_REGION env vars, ~/.aws/config, EC2/ECS metadata, etc.
        this.client = DynamoDbClient.create();
    }

    public void handleCommandLineMode() throws IOException {
        var config = new ExportConfiguration(
                args[1],
                Path.of(args[0]),
                Arrays.copyOfRange(args, 2, args.length)
        );
        new ParallelExporter(config, client).export();
    }

    public void handleInteractiveMode() throws Exception {
        var exportConfig = new InteractiveSession(client).configure();
        new ParallelExporter(exportConfig, client).export();
    }

    @Override
    public void close() {
        client.close();
    }
}
