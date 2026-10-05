package io.kestra.plugin.transform.arrow.reader;

import io.kestra.plugin.transform.arrow.InputFormat;

import java.io.IOException;
import java.nio.file.Path;

public final class RowReaders {
    private RowReaders() {
    }

    public static RowReader open(Path path, InputFormat format, CsvConfig csv) throws IOException {
        try {
            return switch (format) {
                case PARQUET -> new ParquetRowReader(path);
                case ARROW -> ArrowIpcRowReader.open(path);
                case CSV -> new CsvRowReader(path, csv);
                case NDJSON -> new NdjsonRowReader(path);
                case ION -> new IonRowReader(path);
                case AUTO -> throw new IllegalStateException("Input format must be resolved before opening a reader");
            };
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Failed to read " + format + " file '" + path.getFileName() + "': " + e.getMessage(),
                e
            );
        }
    }
}
