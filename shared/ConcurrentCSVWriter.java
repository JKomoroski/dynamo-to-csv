package shared;
import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConcurrentCSVWriter implements AutoCloseable {

    private final ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(10_000);
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final AtomicBoolean done = new AtomicBoolean(false);
    private final CompletableFuture<Void> writerTask;

    ConcurrentCSVWriter(Path outputPath) {
        this.writerTask = CompletableFuture.runAsync(() -> {
            try (var writer = Files.newBufferedWriter(outputPath)) {
                do {
                    String line = queue.poll(1, TimeUnit.SECONDS);
                    if (line != null) {
                        writer.write(line);
                    }
                } while (!done.get() || !queue.isEmpty());
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, executor);
    }

    void writeLineAsync(String line) {
        try {
            queue.put(line);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            done.set(true);
            writerTask.join();
        } finally {
            executor.shutdown();
        }
    }
}
