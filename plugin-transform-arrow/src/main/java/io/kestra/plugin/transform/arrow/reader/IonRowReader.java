package io.kestra.plugin.transform.arrow.reader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.stream.Stream;

/** Reads Kestra Ion records (text or binary) as JSON objects. */
public final class IonRowReader implements RowReader {
    private static final ObjectMapper JSON = JacksonMapper.ofJson();

    private final InputStream input;
    private final Stream<Object> rows;
    private final Iterator<Object> iterator;

    public IonRowReader(Path path) throws IOException {
        this.input = new BufferedInputStream(Files.newInputStream(path), FileSerde.BUFFER_SIZE);
        this.rows = FileSerde.readAll(input).toStream();
        this.iterator = rows.iterator();
    }

    @Override
    public JsonNode next() {
        if (!iterator.hasNext()) {
            return null;
        }
        return JSON.valueToTree(iterator.next());
    }

    @Override
    public void close() throws IOException {
        try {
            rows.close();
        } finally {
            input.close();
        }
    }
}
