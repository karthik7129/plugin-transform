package io.kestra.plugin.transform.arrow;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import jakarta.inject.Inject;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest
class QueryTest {
    private static final ObjectMapper JSON = JacksonMapper.ofJson();
    private static final String FILTER = "select(.total > 100) | {order_id, customer: .customer_name, total}";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void filtersTheSameRowFromEveryFormat() throws Exception {
        RunContext runContext = runContextFactory.of();
        for (String format : List.of("ndjson", "ion", "csv", "parquet", "arrow", "arrows")) {
            Path file = sample(runContext, format);
            URI uri = runContext.storage().putFile(file.toFile());
            Query.Output output = query(runContext, uri, FILTER, false, null);

            assertEquals(1L, output.getProcessedItemsTotal(), format);
            List<Map<String, Object>> rows = readMaps(runContext, output.getUri());
            assertEquals(1, rows.size(), format);
            assertEquals("o1", rows.getFirst().get("order_id"));
            assertEquals("Ada", rows.getFirst().get("customer"));
            assertEquals(150.0d, ((Number) rows.getFirst().get("total")).doubleValue(), format);
        }
    }

    @Test
    void slurpAggregatesEveryRow() throws Exception {
        RunContext runContext = runContextFactory.of();
        URI uri = runContext.storage().putFile(sample(runContext, "ndjson").toFile());

        Query.Output output = query(runContext, uri, "[.[].total] | add / length", true, null);

        assertEquals(1L, output.getProcessedItemsTotal());
        List<Number> values = read(runContext, output.getUri(), new TypeReference<Number>() {
        });
        assertEquals(95.0d, values.getFirst().doubleValue());
    }

    @Test
    void multiOutputExpressionWritesOneRecordPerValue() throws Exception {
        RunContext runContext = runContextFactory.of();
        Path file = runContext.workingDir().createTempFile(".ndjson");
        Files.writeString(file, "{\"tags\":[\"a\",\"b\"]}\n");
        URI uri = runContext.storage().putFile(file.toFile());

        Query.Output output = query(runContext, uri, ".tags[]", false, null);

        assertEquals(List.of("a", "b"), read(runContext, output.getUri(), new TypeReference<String>() {
        }));
    }

    @Test
    void selectMatchingNothingWritesNoRecord() throws Exception {
        RunContext runContext = runContextFactory.of();
        URI uri = runContext.storage().putFile(sample(runContext, "ndjson").toFile());

        Query.Output output = query(runContext, uri, "select(.total > 1000)", false, null);

        assertEquals(0L, output.getProcessedItemsTotal());
        assertEquals(0, readRaw(runContext, output.getUri()).length());
    }

    @Test
    void emptyNdjsonWritesNothing() throws Exception {
        RunContext runContext = runContextFactory.of();
        Path file = runContext.workingDir().createTempFile(".ndjson");
        Files.writeString(file, "");
        URI uri = runContext.storage().putFile(file.toFile());

        Query.Output output = query(runContext, uri, ".", false, null);

        assertEquals(0L, output.getProcessedItemsTotal());
    }

    @Test
    void invalidExpressionFailsBeforeReading() {
        RunContext runContext = runContextFactory.of();
        Query task = Query.builder()
            .from(Property.ofValue("kestra:///not-a-file"))
            .expression(Property.ofValue("select("))
            .build();

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertTrue(error.getMessage().contains("Invalid jq expression"));
    }

    @Test
    void runtimeErrorIncludesTheRowIndex() throws Exception {
        RunContext runContext = runContextFactory.of();
        URI uri = runContext.storage().putFile(sample(runContext, "ndjson").toFile());
        Query task = Query.builder()
            .from(Property.ofValue(uri.toString()))
            .expression(Property.ofValue("if .total < 100 then .total + \"x\" else .order_id end"))
            .build();

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertTrue(error.getMessage().contains("row index 1"), error.getMessage());
    }

    @Test
    void unknownFormatFailsClearly() throws Exception {
        RunContext runContext = runContextFactory.of();
        Path file = runContext.workingDir().createTempFile(".xyz");
        Files.write(file, new byte[]{0x01, 0x02, 0x00, 0x03});
        URI uri = runContext.storage().putFile(file.toFile());
        Query task = Query.builder()
            .from(Property.ofValue(uri.toString()))
            .expression(Property.ofValue("."))
            .build();

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertTrue(error.getMessage().contains("input format") || error.getMessage().contains("inputFormat"), error.getMessage());
    }

    @Test
    void csvOptionsControlDelimiterHeaderAndInference() throws Exception {
        RunContext runContext = runContextFactory.of();
        Path file = runContext.workingDir().createTempFile(".csv");
        Files.writeString(file, "o1;150\no2;40\n");
        URI uri = runContext.storage().putFile(file.toFile());

        Query inferred = Query.builder()
            .from(Property.ofValue(uri.toString()))
            .expression(Property.ofValue("select(.column_1 > 100) | .column_0"))
            .csvOptions(Query.CsvOptions.builder()
                .delimiter(Property.ofValue(";"))
                .header(Property.ofValue(false))
                .build())
            .build();
        Query.Output inferredOutput = inferred.run(runContext);
        assertEquals(List.of("o1"), read(runContext, inferredOutput.getUri(), new TypeReference<String>() {
        }));

        Query strings = Query.builder()
            .from(Property.ofValue(uri.toString()))
            .expression(Property.ofValue(".column_1"))
            .csvOptions(Query.CsvOptions.builder()
                .delimiter(Property.ofValue(";"))
                .header(Property.ofValue(false))
                .inferTypes(Property.ofValue(false))
                .build())
            .build();
        Query.Output stringOutput = strings.run(runContext);
        assertEquals(List.of("150", "40"), read(runContext, stringOutput.getUri(), new TypeReference<String>() {
        }));
    }

    @Test
    void ndjsonOutputIsOneJsonValuePerLine() throws Exception {
        RunContext runContext = runContextFactory.of();
        URI uri = runContext.storage().putFile(sample(runContext, "ndjson").toFile());
        Query task = Query.builder()
            .from(Property.ofValue(uri.toString()))
            .expression(Property.ofValue(FILTER))
            .outputFormat(Property.ofValue(OutputFormat.NDJSON))
            .build();

        Query.Output output = task.run(runContext);
        String body = readRaw(runContext, output.getUri()).strip();
        assertEquals("{\"order_id\":\"o1\",\"customer\":\"Ada\",\"total\":150}", body);
    }

    @Test
    void arrowTypesBecomeJsonValues() throws Exception {
        RunContext runContext = runContextFactory.of();
        Path file = runContext.workingDir().createTempFile(".arrow");
        writeRichArrow(file);
        URI uri = runContext.storage().putFile(file.toFile());

        Query.Output output = query(runContext, uri, ".", false, null);
        assertEquals(1L, output.getProcessedItemsTotal());
        Map<String, Object> row = readMaps(runContext, output.getUri()).getFirst();

        assertEquals("o1", row.get("order_id"));
        assertEquals("Paris", ((Map<?, ?>) row.get("address")).get("city"));
        assertEquals(List.of("a", "b"), row.get("tags"));
        assertEquals("2024-01-02", row.get("day"));
        assertEquals(new BigDecimal("12345.67"), new BigDecimal(row.get("amount").toString()));
        assertEquals(new BigInteger("9223372036854775808"), new BigInteger(row.get("big").toString()));
        assertEquals(Base64.getEncoder().encodeToString(new byte[]{1, 2, 3, (byte) 255}), row.get("payload"));
        assertEquals(null, row.get("note"));
    }

    @Test
    void parquetNestedRecordsAndListsAreObjectsAndArrays() throws Exception {
        RunContext runContext = runContextFactory.of();
        Path file = runContext.workingDir().createTempFile(".parquet");
        writeNestedParquet(file);
        URI uri = runContext.storage().putFile(file.toFile());

        Query.Output output = query(runContext, uri, ".", false, null);
        Map<String, Object> row = readMaps(runContext, output.getUri()).getFirst();
        assertEquals("o1", row.get("order_id"));
        assertEquals(List.of("a", "b"), row.get("tags"));
        assertEquals("Paris", ((Map<?, ?>) row.get("address")).get("city"));
    }

    @Test
    void streamsALargeNdjsonFile() throws Exception {
        RunContext runContext = runContextFactory.of();
        Path file = runContext.workingDir().createTempFile(".ndjson");
        StringBuilder body = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            body.append("{\"n\":").append(i).append("}\n");
        }
        Files.writeString(file, body.toString());
        URI uri = runContext.storage().putFile(file.toFile());

        Query.Output output = query(runContext, uri, "select(.n >= 900) | .n", false, null);
        assertEquals(100L, output.getProcessedItemsTotal());
    }

    private Query.Output query(RunContext runContext, URI uri, String expression, boolean slurp, Query.CsvOptions csv) throws Exception {
        Query.QueryBuilder<?, ?> builder = Query.builder()
            .from(Property.ofValue(uri.toString()))
            .expression(Property.ofValue(expression))
            .slurp(Property.ofValue(slurp));
        if (csv != null) {
            builder.csvOptions(csv);
        }
        return builder.build().run(runContext);
    }

    private Path sample(RunContext runContext, String format) throws Exception {
        Path file = runContext.workingDir().createTempFile("." + format);
        switch (format) {
            case "ndjson" -> Files.writeString(file, """
                {"order_id":"o1","customer_name":"Ada","total":150}
                {"order_id":"o2","customer_name":"Grace","total":40}
                """);
            case "csv" -> Files.writeString(file, """
                order_id,customer_name,total
                o1,Ada,150
                o2,Grace,40
                """);
            case "ion" -> {
                try (OutputStream output = new BufferedOutputStream(Files.newOutputStream(file))) {
                    FileSerde.writeAll(output, Flux.just(
                        Map.of("order_id", "o1", "customer_name", "Ada", "total", 150),
                        Map.of("order_id", "o2", "customer_name", "Grace", "total", 40)
                    )).block();
                }
            }
            case "parquet" -> writeOrdersParquet(file);
            case "arrow" -> writeOrdersArrow(file, false);
            case "arrows" -> writeOrdersArrow(file, true);
            default -> throw new IllegalArgumentException(format);
        }
        return file;
    }

    private static void writeOrdersParquet(Path file) throws Exception {
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse("""
            {
              "type": "record",
              "name": "Order",
              "fields": [
                {"name": "order_id", "type": "string"},
                {"name": "customer_name", "type": "string"},
                {"name": "total", "type": "double"}
              ]
            }
            """);
        Files.deleteIfExists(file);
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new LocalOutputFile(file))
            .withSchema(schema)
            .withConf(new PlainParquetConfiguration())
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build()) {
            writer.write(order(schema, "o1", "Ada", 150.0d));
            writer.write(order(schema, "o2", "Grace", 40.0d));
        }
    }

    private static GenericRecord order(org.apache.avro.Schema schema, String id, String name, double total) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("order_id", id);
        record.put("customer_name", name);
        record.put("total", total);
        return record;
    }

    private static void writeNestedParquet(Path file) throws Exception {
        org.apache.avro.Schema address = org.apache.avro.Schema.createRecord("Address", null, null, false);
        address.setFields(List.of(new org.apache.avro.Schema.Field("city", org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING))));
        org.apache.avro.Schema schema = org.apache.avro.Schema.createRecord("Order", null, null, false);
        schema.setFields(List.of(
            new org.apache.avro.Schema.Field("order_id", org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING)),
            new org.apache.avro.Schema.Field("tags", org.apache.avro.Schema.createArray(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING))),
            new org.apache.avro.Schema.Field("address", address)
        ));
        GenericRecord addressRecord = new GenericData.Record(address);
        addressRecord.put("city", "Paris");
        GenericRecord record = new GenericData.Record(schema);
        record.put("order_id", "o1");
        record.put("tags", List.of("a", "b"));
        record.put("address", addressRecord);
        Files.deleteIfExists(file);
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new LocalOutputFile(file))
            .withSchema(schema)
            .withConf(new PlainParquetConfiguration())
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build()) {
            writer.write(record);
        }
    }

    private static void writeOrdersArrow(Path file, boolean stream) throws Exception {
        Schema schema = new Schema(List.of(
            Field.nullable("order_id", new ArrowType.Utf8()),
            Field.nullable("customer_name", new ArrowType.Utf8()),
            Field.nullable("total", new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE))
        ));
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            if (stream) {
                try (OutputStream output = Files.newOutputStream(file);
                     ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(output))) {
                    writer.start();
                    writeOrderBatch(root, "o1", "Ada", 150.0d);
                    writer.writeBatch();
                    writeOrderBatch(root, "o2", "Grace", 40.0d);
                    writer.writeBatch();
                    writer.end();
                }
            } else {
                try (ArrowFileWriter writer = new ArrowFileWriter(root, null, Files.newByteChannel(
                    file, java.nio.file.StandardOpenOption.WRITE, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.TRUNCATE_EXISTING))) {
                    writer.start();
                    writeOrderBatch(root, "o1", "Ada", 150.0d);
                    writer.writeBatch();
                    writeOrderBatch(root, "o2", "Grace", 40.0d);
                    writer.writeBatch();
                    writer.end();
                }
            }
        }
    }

    private static void writeOrderBatch(VectorSchemaRoot root, String id, String name, double total) {
        ((VarCharVector) root.getVector("order_id")).setSafe(0, id.getBytes(StandardCharsets.UTF_8));
        ((VarCharVector) root.getVector("customer_name")).setSafe(0, name.getBytes(StandardCharsets.UTF_8));
        ((Float8Vector) root.getVector("total")).setSafe(0, total);
        root.setRowCount(1);
    }

    private static void writeRichArrow(Path file) throws Exception {
        Field address = new Field(
            "address",
            org.apache.arrow.vector.types.pojo.FieldType.nullable(new ArrowType.Struct()),
            List.of(Field.nullable("city", new ArrowType.Utf8()))
        );
        Field tags = new Field(
            "tags",
            org.apache.arrow.vector.types.pojo.FieldType.nullable(new ArrowType.List()),
            List.of(Field.nullable("item", new ArrowType.Utf8()))
        );
        Schema schema = new Schema(List.of(
            Field.nullable("order_id", new ArrowType.Utf8()),
            address,
            tags,
            Field.nullable("day", new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY)),
            Field.nullable("amount", new ArrowType.Decimal(10, 2, 128)),
            Field.nullable("big", new ArrowType.Int(64, false)),
            Field.nullable("payload", new ArrowType.Binary()),
            Field.nullable("note", new ArrowType.Utf8())
        ));
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
             ArrowFileWriter writer = new ArrowFileWriter(root, null, Files.newByteChannel(
                 file, java.nio.file.StandardOpenOption.WRITE, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.TRUNCATE_EXISTING))) {
            root.allocateNew();
            ((VarCharVector) root.getVector("order_id")).setSafe(0, "o1".getBytes(StandardCharsets.UTF_8));

            StructVector addressVector = (StructVector) root.getVector("address");
            ((VarCharVector) addressVector.getChild("city")).setSafe(0, "Paris".getBytes(StandardCharsets.UTF_8));
            addressVector.setIndexDefined(0);

            ListVector tagsVector = (ListVector) root.getVector("tags");
            VarCharVector tagData = (VarCharVector) tagsVector.getDataVector();
            tagsVector.startNewValue(0);
            tagData.setSafe(0, "a".getBytes(StandardCharsets.UTF_8));
            tagData.setSafe(1, "b".getBytes(StandardCharsets.UTF_8));
            tagsVector.endValue(0, 2);

            ((DateDayVector) root.getVector("day")).set(0, (int) LocalDate.of(2024, 1, 2).toEpochDay());
            ((DecimalVector) root.getVector("amount")).setSafe(0, new BigDecimal("12345.67"));
            ((UInt8Vector) root.getVector("big")).set(0, Long.parseUnsignedLong("9223372036854775808"));
            ((VarBinaryVector) root.getVector("payload")).setSafe(0, new byte[]{1, 2, 3, (byte) 255});
            ((VarCharVector) root.getVector("note")).setNull(0);
            root.setRowCount(1);

            writer.start();
            writer.writeBatch();
            writer.end();
        }
    }

    private List<Map<String, Object>> readMaps(RunContext runContext, URI uri) throws Exception {
        return read(runContext, uri, new TypeReference<Map<String, Object>>() {
        });
    }

    private <T> List<T> read(RunContext runContext, URI uri, TypeReference<T> type) throws Exception {
        try (InputStream input = runContext.storage().getFile(uri)) {
            return FileSerde.readAll(new InputStreamReader(input), type).collectList().block();
        }
    }

    private String readRaw(RunContext runContext, URI uri) throws Exception {
        try (InputStream input = runContext.storage().getFile(uri)) {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
