package io.kestra.plugin.transform.arrow.reader;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

/** Pull-based reader of one JSON object per input row. */
public interface RowReader extends AutoCloseable {
    /**
     * @return the next row, or {@code null} when the input is exhausted
     */
    JsonNode next() throws IOException;

    @Override
    void close() throws IOException;
}
