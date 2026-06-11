package shared;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

public class DynamoExporter implements AutoCloseable {
    private final String[] args;
    private final DynamoDbClient client;

    public DynamoExporter(String[] args) {
        this.args = args;
        this.client = DynamoDbClient.builder().region(Region.US_EAST_2).build();
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
