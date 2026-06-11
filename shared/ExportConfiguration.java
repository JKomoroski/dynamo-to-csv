package shared;
import java.nio.file.Path;

public record ExportConfiguration(String tableName, Path outputPath, String[] attributes) {}
