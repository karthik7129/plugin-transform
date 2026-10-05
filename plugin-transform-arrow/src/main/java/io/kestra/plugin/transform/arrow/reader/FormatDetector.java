package io.kestra.plugin.transform.arrow.reader;

import io.kestra.plugin.transform.arrow.InputFormat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

public final class FormatDetector {
    private static final byte[] PARQUET_MAGIC = {'P', 'A', 'R', '1'};
    private static final byte[] ARROW_MAGIC = {'A', 'R', 'R', 'O', 'W', '1'};
    private static final byte[] ION_BINARY_MAGIC = {(byte) 0xE0, 0x01, 0x00, (byte) 0xEA};

    private FormatDetector() {
    }

    public static InputFormat detect(Path path, InputFormat requested) throws IOException {
        if (requested != null && requested != InputFormat.AUTO) {
            return requested;
        }

        InputFormat byExtension = byExtension(path.getFileName().toString());
        if (byExtension != null) {
            return byExtension;
        }

        byte[] prefix = readPrefix(path, 64);
        InputFormat byMagic = byMagic(prefix);
        if (byMagic != null) {
            return byMagic;
        }

        throw new IllegalArgumentException(
            "Unrecognized file '" + path.getFileName() + "'. Set inputFormat to PARQUET, ARROW, CSV, NDJSON, or ION."
        );
    }

    /**
     * Arrow IPC streams do not share the file magic. Callers that already know the format is Arrow
     * use this to pick a reader.
     */
    public static boolean isArrowFile(Path path) throws IOException {
        return startsWith(readPrefix(path, ARROW_MAGIC.length), ARROW_MAGIC);
    }

    private static InputFormat byExtension(String fileName) {
        int dot = fileName.lastIndexOf('.');
        if (dot < 0 || dot == fileName.length() - 1) {
            return null;
        }
        String ext = fileName.substring(dot + 1).toLowerCase(Locale.ROOT);
        return switch (ext) {
            case "parquet", "pq" -> InputFormat.PARQUET;
            case "arrow", "arrows", "ipc", "feather" -> InputFormat.ARROW;
            case "csv", "tsv" -> InputFormat.CSV;
            case "ndjson", "jsonl", "json" -> InputFormat.NDJSON;
            case "ion" -> InputFormat.ION;
            default -> null;
        };
    }

    private static InputFormat byMagic(byte[] prefix) {
        if (prefix.length == 0) {
            return null;
        }
        if (startsWith(prefix, PARQUET_MAGIC)) {
            return InputFormat.PARQUET;
        }
        if (startsWith(prefix, ARROW_MAGIC) || isArrowStream(prefix)) {
            return InputFormat.ARROW;
        }
        if (startsWith(prefix, ION_BINARY_MAGIC)) {
            return InputFormat.ION;
        }

        String text = new String(prefix, StandardCharsets.ISO_8859_1).stripLeading();
        if (text.startsWith("{") || text.startsWith("[")) {
            return InputFormat.NDJSON;
        }
        if (text.startsWith("$ion")) {
            return InputFormat.ION;
        }
        if (isText(prefix)) {
            return InputFormat.CSV;
        }
        return null;
    }

    private static boolean isArrowStream(byte[] prefix) {
        return prefix.length >= 4
            && (prefix[0] & 0xFF) == 0xFF
            && (prefix[1] & 0xFF) == 0xFF
            && (prefix[2] & 0xFF) == 0xFF
            && (prefix[3] & 0xFF) == 0xFF;
    }

    private static boolean isText(byte[] prefix) {
        for (byte value : prefix) {
            int b = value & 0xFF;
            if (b != '\t' && b != '\n' && b != '\r' && (b < 32 || b > 126)) {
                return false;
            }
        }
        return true;
    }

    private static boolean startsWith(byte[] value, byte[] magic) {
        if (value.length < magic.length) {
            return false;
        }
        for (int i = 0; i < magic.length; i++) {
            if (value[i] != magic[i]) {
                return false;
            }
        }
        return true;
    }

    private static byte[] readPrefix(Path path, int length) throws IOException {
        byte[] buffer = new byte[length];
        try (InputStream input = Files.newInputStream(path)) {
            int read = input.read(buffer);
            if (read <= 0) {
                return new byte[0];
            }
            if (read == buffer.length) {
                return buffer;
            }
            byte[] trimmed = new byte[read];
            System.arraycopy(buffer, 0, trimmed, 0, read);
            return trimmed;
        }
    }
}
