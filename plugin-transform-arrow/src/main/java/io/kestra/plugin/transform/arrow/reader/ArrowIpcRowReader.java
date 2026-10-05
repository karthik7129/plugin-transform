package io.kestra.plugin.transform.arrow.reader;

import com.fasterxml.jackson.databind.JsonNode;
import io.kestra.plugin.transform.arrow.convert.ArrowValueConverter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;

/** Reads an Arrow IPC file (random access) or stream, one record batch at a time. */
public final class ArrowIpcRowReader implements RowReader {
    private final BufferAllocator allocator;
    private final ArrowReader reader;
    private final SeekableByteChannel channel;
    private final InputStream stream;

    private VectorSchemaRoot root;
    private Map<Long, Dictionary> dictionaries = Collections.emptyMap();
    private int nextRow;
    private boolean batchOpen;
    private boolean finished;

    private ArrowIpcRowReader(BufferAllocator allocator, ArrowReader reader, SeekableByteChannel channel, InputStream stream) {
        this.allocator = allocator;
        this.reader = reader;
        this.channel = channel;
        this.stream = stream;
    }

    public static ArrowIpcRowReader open(Path path) throws IOException {
        BufferAllocator allocator = new RootAllocator();
        SeekableByteChannel channel = null;
        InputStream stream = null;
        try {
            ArrowReader reader;
            if (FormatDetector.isArrowFile(path)) {
                channel = Files.newByteChannel(path);
                reader = new ArrowFileReader(channel, allocator);
            } else {
                stream = Files.newInputStream(path);
                reader = new ArrowStreamReader(stream, allocator);
            }
            return new ArrowIpcRowReader(allocator, reader, channel, stream);
        } catch (RuntimeException | IOException e) {
            if (channel != null) {
                channel.close();
            }
            if (stream != null) {
                stream.close();
            }
            allocator.close();
            throw e;
        }
    }

    @Override
    public JsonNode next() throws IOException {
        while (!finished) {
            if (!batchOpen) {
                if (!reader.loadNextBatch()) {
                    finished = true;
                    return null;
                }
                root = reader.getVectorSchemaRoot();
                dictionaries = reader.getDictionaryVectors();
                if (dictionaries == null) {
                    dictionaries = Collections.emptyMap();
                }
                nextRow = 0;
                batchOpen = true;
            }
            if (nextRow >= root.getRowCount()) {
                batchOpen = false;
                continue;
            }
            // Materialize before the next batch reuses these vectors.
            JsonNode row = ArrowValueConverter.toRow(root, nextRow, dictionaries);
            nextRow++;
            return row;
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        try {
            reader.close();
        } finally {
            try {
                if (channel != null) {
                    channel.close();
                }
            } finally {
                try {
                    if (stream != null) {
                        stream.close();
                    }
                } finally {
                    allocator.close();
                }
            }
        }
    }
}
