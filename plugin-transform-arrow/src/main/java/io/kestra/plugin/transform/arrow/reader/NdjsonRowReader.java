package io.kestra.plugin.transform.arrow.reader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/** Streams concatenated JSON values, including one object per line. */
public final class NdjsonRowReader implements RowReader {
    private static final ObjectMapper JSON = JacksonMapper.ofJson();

    private final InputStream input;
    private final MappingIterator<JsonNode> values;

    public NdjsonRowReader(Path path) throws IOException {
        this.input = new BufferedInputStream(Files.newInputStream(path), FileSerde.BUFFER_SIZE);
        this.values = JSON.readerFor(JsonNode.class).readValues(input);
    }

    @Override
    public JsonNode next() throws IOException {
        if (!values.hasNextValue()) {
            return null;
        }
        try {
            return values.nextValue();
        } catch (JsonProcessingException e) {
            throw new IOException("Invalid NDJSON near " + e.getLocation() + ": " + e.getOriginalMessage(), e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            values.close();
        } finally {
            input.close();
        }
    }
}
