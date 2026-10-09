package shared;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.zip.*;

public class AttributeJoiner {

    private final List<SegmentExporter.SegmentResult> results;
    private final ExportConfiguration config;

    AttributeJoiner(List<SegmentExporter.SegmentResult> results, ExportConfiguration config) {
        this.results = results;
        this.config = config;
    }

    void join() throws IOException {
        List<String> globalAttributes = mergeAttributes();
        Path resolvedOutput = resolveOutputPath();

        try (var writer = openWriter(resolvedOutput)) {
            writer.write(String.join(",", globalAttributes) + "\n");
            for (var result : results) {
                appendSegment(writer, result, globalAttributes);
            }
        }

        deleteTemp();
        System.out.println("Export completed: " + resolvedOutput);
    }

    private List<String> mergeAttributes() {
        if (!config.allAttributes()) {
            // Preserve the user-specified attribute order exactly
            return List.of(config.attributes());
        }
        // All-attributes: alphabetical sort for deterministic, consistent output
        TreeSet<String> merged = new TreeSet<>();
        for (var result : results) {
            merged.addAll(result.attributes());
        }
        return new ArrayList<>(merged);
    }

    private Path resolveOutputPath() {
        String name = config.outputPath().getFileName().toString();
        return switch (config.compressionType()) {
            case GZIP -> name.endsWith(".gz") ? config.outputPath() : config.outputPath().resolveSibling(name + ".gz");
            case ZIP  -> name.endsWith(".zip") ? config.outputPath() : config.outputPath().resolveSibling(name + ".zip");
            case NONE -> config.outputPath();
        };
    }

    private BufferedWriter openWriter(Path path) throws IOException {
        OutputStream base = Files.newOutputStream(path);
        OutputStream compressed = switch (config.compressionType()) {
            case GZIP -> new GZIPOutputStream(base);
            case ZIP  -> {
                var zos = new ZipOutputStream(base);
                String innerName = path.getFileName().toString().replaceAll("\\.zip$", ".csv");
                zos.putNextEntry(new ZipEntry(innerName));
                yield zos;
            }
            case NONE -> base;
        };
        return new BufferedWriter(new OutputStreamWriter(compressed));
    }

    private void appendSegment(BufferedWriter writer, SegmentExporter.SegmentResult result, List<String> globalAttributes) throws IOException {
        List<String> segmentAttributes = result.attributes();
        // Build index map: globalIndex -> segmentIndex (-1 if segment doesn't have this attribute)
        int[] indexMap = new int[globalAttributes.size()];
        for (int i = 0; i < globalAttributes.size(); i++) {
            indexMap[i] = segmentAttributes.indexOf(globalAttributes.get(i));
        }

        try (var reader = Files.newBufferedReader(result.tempFile())) {
            reader.readLine(); // skip mini-header
            String line;
            while ((line = reader.readLine()) != null) {
                String[] fields = parseCsvLine(line);
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < indexMap.length; i++) {
                    if (i > 0) sb.append(",");
                    int srcIdx = indexMap[i];
                    String value = srcIdx >= 0 && srcIdx < fields.length ? fields[srcIdx] : "";
                    sb.append(escapeCSV(value));
                }
                writer.write(sb + "\n");
            }
        }
    }

    private String[] parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        boolean inQuotes = false;
        StringBuilder current = new StringBuilder();
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        current.append('"');
                        i++;
                    } else {
                        inQuotes = false;
                    }
                } else {
                    current.append(c);
                }
            } else {
                if (c == '"') {
                    inQuotes = true;
                } else if (c == ',') {
                    fields.add(current.toString());
                    current.setLength(0);
                } else {
                    current.append(c);
                }
            }
        }
        fields.add(current.toString());
        return fields.toArray(new String[0]);
    }

    private String escapeCSV(String value) {
        if (value == null || value.isEmpty()) return "";
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }

    private void deleteTemp() {
        for (var result : results) {
            try {
                Files.deleteIfExists(result.tempFile());
            } catch (IOException e) {
                System.err.println("Warning: could not delete temp file: " + result.tempFile());
            }
        }
    }
}
