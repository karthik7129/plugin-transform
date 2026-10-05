package io.kestra.plugin.transform.arrow.reader;

import io.kestra.plugin.transform.arrow.InputFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FormatDetectorTest {
    @TempDir
    Path temp;

    @Test
    void extensionWinsOverMagic() throws Exception {
        Path csv = temp.resolve("orders.csv");
        Files.write(csv, new byte[]{'P', 'A', 'R', '1', 0});
        assertEquals(InputFormat.CSV, FormatDetector.detect(csv, InputFormat.AUTO));
    }

    @Test
    void explicitFormatWinsOverExtension() throws Exception {
        Path csv = temp.resolve("orders.csv");
        Files.writeString(csv, "a,b\n1,2\n");
        assertEquals(InputFormat.PARQUET, FormatDetector.detect(csv, InputFormat.PARQUET));
    }

    @Test
    void magicBytesCoverTheFormatsWithoutAnExtension() throws Exception {
        assertEquals(InputFormat.PARQUET, FormatDetector.detect(bytes("noext", new byte[]{'P', 'A', 'R', '1'}), InputFormat.AUTO));
        assertEquals(InputFormat.ARROW, FormatDetector.detect(bytes("noext2", "ARROW1".getBytes(StandardCharsets.US_ASCII)), InputFormat.AUTO));
        assertEquals(InputFormat.ARROW, FormatDetector.detect(bytes("noext3", new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}), InputFormat.AUTO));
        assertEquals(InputFormat.ION, FormatDetector.detect(bytes("noext4", new byte[]{(byte) 0xE0, 0x01, 0x00, (byte) 0xEA}), InputFormat.AUTO));
        assertEquals(InputFormat.NDJSON, FormatDetector.detect(bytes("noext5", "{\"a\":1}".getBytes(StandardCharsets.UTF_8)), InputFormat.AUTO));
        assertEquals(InputFormat.CSV, FormatDetector.detect(bytes("noext6", "a,b\n1,2\n".getBytes(StandardCharsets.UTF_8)), InputFormat.AUTO));
    }

    @Test
    void knownExtensions() throws Exception {
        assertEquals(InputFormat.PARQUET, FormatDetector.detect(touch("t.parquet"), InputFormat.AUTO));
        assertEquals(InputFormat.ARROW, FormatDetector.detect(touch("t.feather"), InputFormat.AUTO));
        assertEquals(InputFormat.CSV, FormatDetector.detect(touch("t.tsv"), InputFormat.AUTO));
        assertEquals(InputFormat.NDJSON, FormatDetector.detect(touch("t.jsonl"), InputFormat.AUTO));
        assertEquals(InputFormat.ION, FormatDetector.detect(touch("t.ion"), InputFormat.AUTO));
    }

    @Test
    void unknownBytesFailWithAClearMessage() throws Exception {
        Path file = bytes("blob.xyz", new byte[]{0x01, 0x02, 0x00, 0x03});
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> FormatDetector.detect(file, InputFormat.AUTO));
        assertEquals(true, error.getMessage().contains("inputFormat"));
    }

    private Path touch(String name) throws Exception {
        Path path = temp.resolve(name);
        Files.writeString(path, "");
        return path;
    }

    private Path bytes(String name, byte[] content) throws Exception {
        Path path = temp.resolve(name);
        Files.write(path, content);
        return path;
    }
}
