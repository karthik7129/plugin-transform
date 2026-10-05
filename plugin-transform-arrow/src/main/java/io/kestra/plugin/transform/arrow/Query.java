package io.kestra.plugin.transform.arrow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.transform.arrow.jq.JqEvaluator;
import io.kestra.plugin.transform.arrow.reader.CsvConfig;
import io.kestra.plugin.transform.arrow.reader.FormatDetector;
import io.kestra.plugin.transform.arrow.reader.RowReader;
import io.kestra.plugin.transform.arrow.reader.RowReaders;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query a Parquet, Arrow, CSV, NDJSON, or Ion file with jq",
    description = """
        Applies a [jq](https://jqlang.org/) expression to columnar or row-oriented data, the same idea as [aq](https://github.com/Anaethelion/aq).
        Each input row is converted to a JSON object, the expression runs on that object, and every jq output is written as a record.
        Set `slurp` to collect every row into one array before evaluating, which is what aggregates need. Slurp holds the whole file in memory.

        The expression is jq 1.6, implemented by jackson-jq. `$ENV`, `input`/`inputs`, and some path builtins are not available.
        An expression that returns nothing (for example `select` matching no row) writes no record. An output that is JSON null is also omitted, because Ion records cannot store a null row.
        Temporal values are written as ISO-8601 strings. Arrow and Parquet maps are arrays of `{"key", "value"}` objects. Binary values are base64 strings.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Keep high-value orders from a Parquet file",
            full = true,
            code = """
                id: arrow_query
                namespace: company.team

                tasks:
                  - id: download
                    type: io.kestra.plugin.core.http.Download
                    uri: https://huggingface.co/datasets/kestra/datasets/resolve/main/parquet/orders.parquet

                  - id: high_value_orders
                    type: io.kestra.plugin.transform.arrow.Query
                    from: "{{ outputs.download.uri }}"
                    expression: 'select(.total > 100) | {order_id, customer: .customer_name, total}'
                """
        ),
        @Example(
            title = "Aggregate every row with jq slurp",
            full = true,
            code = """
                id: arrow_query_slurp
                namespace: company.team

                tasks:
                  - id: download
                    type: io.kestra.plugin.core.http.Download
                    uri: https://huggingface.co/datasets/kestra/datasets/resolve/main/json/orders.ndjson

                  - id: avg_total
                    type: io.kestra.plugin.transform.arrow.Query
                    from: "{{ outputs.download.uri }}"
                    slurp: true
                    expression: '[.[].total] | add / length'
                """
        ),
        @Example(
            title = "Query a semicolon-separated CSV file",
            full = true,
            code = """
                id: arrow_query_csv
                namespace: company.team

                tasks:
                  - id: write_csv
                    type: io.kestra.plugin.core.storage.Write
                    content: |
                      order_id;customer_name;total
                      o1;Ada;150
                      o2;Grace;40
                    extension: .csv

                  - id: high_value_orders
                    type: io.kestra.plugin.transform.arrow.Query
                    from: "{{ outputs.write_csv.uri }}"
                    inputFormat: CSV
                    expression: 'select(.total > 100) | {order_id, total}'
                    csvOptions:
                      delimiter: ";"
                """
        )
    }
)
public class Query extends Task implements RunnableTask<Query.Output> {
    private static final ObjectMapper JSON = JacksonMapper.ofJson();

    @Schema(
        title = "Source file",
        description = "A `kestra://` internal storage URI. Parquet and Arrow IPC files are copied locally because those formats need random access."
    )
    @NotNull
    @PluginProperty(internalStorageURI = true, group = "main")
    private Property<String> from;

    @Schema(
        title = "jq expression",
        description = """
            jq expression evaluated once per row, or once over the whole array when `slurp` is true.
            An invalid expression fails the task before any row is read. A runtime failure includes the zero-based row index.
            """
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> expression;

    @Schema(
        title = "Input format",
        description = """
            How to read `from`. `AUTO` uses the file extension (`.parquet`, `.arrow`/`.ipc`/`.feather`, `.csv`/`.tsv`, `.ndjson`/`.jsonl`/`.json`, `.ion`) and otherwise the leading magic bytes (`PAR1`, `ARROW1`, the Arrow stream marker, or Ion's binary header).
            """
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<InputFormat> inputFormat = Property.ofValue(InputFormat.AUTO);

    @Schema(
        title = "Slurp rows into one array",
        description = "When `true`, every row is collected into a JSON array and the expression runs once on that array, like `jq -s` or `aq -s`. Required for aggregates. The whole input is held in memory."
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> slurp = Property.ofValue(false);

    @Schema(
        title = "Output format",
        description = "`ION` writes Amazon Ion records for downstream Kestra tasks. `NDJSON` writes one JSON value per line."
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<OutputFormat> outputFormat = Property.ofValue(OutputFormat.ION);

    @Schema(
        title = "CSV options",
        description = "Used only when the input format is CSV."
    )
    @PluginProperty(group = "advanced")
    private CsvOptions csvOptions;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String renderedExpression = runContext.render(this.expression).as(String.class).orElseThrow();
        JqEvaluator evaluator = new JqEvaluator(renderedExpression);

        URI source = new URI(runContext.render(this.from).as(String.class).orElseThrow());
        InputFormat requested = runContext.render(this.inputFormat).as(InputFormat.class).orElse(InputFormat.AUTO);
        boolean slurped = runContext.render(this.slurp).as(Boolean.class).orElse(false);
        OutputFormat renderedOutput = runContext.render(this.outputFormat).as(OutputFormat.class).orElse(OutputFormat.ION);

        String suffix = suffix(source);
        Path localInput = runContext.workingDir().createTempFile(suffix.isEmpty() ? ".bin" : suffix);
        Path localOutput = runContext.workingDir().createTempFile(renderedOutput == OutputFormat.NDJSON ? ".ndjson" : ".ion");
        try {
            try (InputStream input = runContext.storage().getFile(source);
                 OutputStream output = new BufferedOutputStream(Files.newOutputStream(localInput), FileSerde.BUFFER_SIZE)) {
                input.transferTo(output);
            }

            InputFormat detected;
            try {
                detected = FormatDetector.detect(localInput, requested);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Unable to detect the input format of '" + source + "'. " + e.getMessage(), e);
            }

            CsvConfig csv = csvConfig(runContext);
            long written;
            try (RowReader reader = RowReaders.open(localInput, detected, csv)) {
                written = slurped
                    ? writeSlurped(localOutput, renderedOutput, reader, evaluator)
                    : writeStreaming(localOutput, renderedOutput, reader, evaluator);
            }

            return Output.builder()
                .uri(runContext.storage().putFile(localOutput.toFile()))
                .processedItemsTotal(written)
                .build();
        } finally {
            Files.deleteIfExists(localInput);
            Files.deleteIfExists(localOutput);
        }
    }

    private long writeSlurped(Path output, OutputFormat format, RowReader reader, JqEvaluator evaluator) throws Exception {
        ArrayNode all = JSON.createArrayNode();
        JsonNode row;
        while ((row = reader.next()) != null) {
            all.add(row);
        }
        final List<JsonNode> values;
        try {
            values = evaluator.apply(all);
        } catch (Exception e) {
            throw new IllegalArgumentException("jq expression failed while evaluating slurped input: " + e.getMessage(), e);
        }
        if (format == OutputFormat.NDJSON) {
            return writeNdjson(output, values);
        }
        return writeIon(output, Flux.fromIterable(values).map(Query::toIonValue));
    }

    private long writeStreaming(Path output, OutputFormat format, RowReader reader, JqEvaluator evaluator) throws Exception {
        if (format == OutputFormat.NDJSON) {
            long count = 0;
            long index = 0;
            try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
                JsonNode row;
                while ((row = reader.next()) != null) {
                    for (JsonNode value : applyRow(evaluator, row, index)) {
                        if (value == null || value.isNull() || value.isMissingNode()) {
                            continue;
                        }
                        writer.write(JSON.writeValueAsString(value));
                        writer.newLine();
                        count++;
                    }
                    index++;
                }
            }
            return count;
        }

        Flux<Object> values = Flux.create(sink -> emitRows(sink, reader, evaluator), FluxSink.OverflowStrategy.BUFFER);
        return writeIon(output, values);
    }

    private static void emitRows(FluxSink<Object> sink, RowReader reader, JqEvaluator evaluator) {
        long index = 0;
        try {
            JsonNode row;
            while ((row = reader.next()) != null) {
                for (JsonNode value : applyRow(evaluator, row, index)) {
                    Object ion = toIonValue(value);
                    if (ion != null) {
                        sink.next(ion);
                    }
                }
                index++;
            }
            sink.complete();
        } catch (Exception e) {
            sink.error(e);
        }
    }

    private static List<JsonNode> applyRow(JqEvaluator evaluator, JsonNode row, long index) {
        try {
            return evaluator.apply(row);
        } catch (Exception e) {
            throw new IllegalArgumentException("jq expression failed at row index " + index + ": " + e.getMessage(), e);
        }
    }

    private static long writeNdjson(Path output, List<JsonNode> values) throws Exception {
        long count = 0;
        try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            for (JsonNode value : values) {
                if (value == null || value.isNull() || value.isMissingNode()) {
                    continue;
                }
                writer.write(JSON.writeValueAsString(value));
                writer.newLine();
                count++;
            }
        }
        return count;
    }

    private static long writeIon(Path output, Flux<Object> values) throws Exception {
        try (OutputStream stream = new BufferedOutputStream(Files.newOutputStream(output), FileSerde.BUFFER_SIZE)) {
            Long count = FileSerde.writeAll(stream, values).block();
            return count == null ? 0L : count;
        }
    }

    private static Object toIonValue(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return null;
        }
        return JSON.convertValue(node, Object.class);
    }

    private CsvConfig csvConfig(RunContext runContext) throws Exception {
        CsvOptions options = this.csvOptions == null ? CsvOptions.builder().build() : this.csvOptions;
        String delimiter = runContext.render(options.getDelimiter()).as(String.class).orElse(",");
        boolean header = runContext.render(options.getHeader()).as(Boolean.class).orElse(true);
        String charsetName = runContext.render(options.getCharset()).as(String.class).orElse(StandardCharsets.UTF_8.name());
        boolean inferTypes = runContext.render(options.getInferTypes()).as(Boolean.class).orElse(true);
        Charset charset;
        try {
            charset = Charset.forName(charsetName);
        } catch (Exception e) {
            throw new IllegalArgumentException("Unsupported csvOptions.charset '" + charsetName + "'", e);
        }
        return new CsvConfig(parseDelimiter(delimiter), header, charset, inferTypes);
    }

    private static char parseDelimiter(String value) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("csvOptions.delimiter must be a single character");
        }
        if ("\\t".equals(value) || "\t".equals(value)) {
            return '\t';
        }
        if (value.length() != 1) {
            throw new IllegalArgumentException("csvOptions.delimiter must be a single character");
        }
        return value.charAt(0);
    }

    private static String suffix(URI source) {
        String path = source.getPath();
        if (path == null || path.isEmpty()) {
            return "";
        }
        String name = path.substring(path.lastIndexOf('/') + 1);
        int dot = name.lastIndexOf('.');
        if (dot < 0 || dot == name.length() - 1) {
            return "";
        }
        return name.substring(dot);
    }

    @Builder
    @Getter
    @Setter
    @NoArgsConstructor
    @lombok.AllArgsConstructor
    public static class CsvOptions {
        @Schema(
            title = "Field delimiter",
            description = "A single character. Use a tab character, or the two characters `\\t`, for TSV."
        )
        @Builder.Default
        @PluginProperty(group = "advanced")
        private Property<String> delimiter = Property.ofValue(",");

        @Schema(
            title = "First row is a header",
            description = "When `true`, the first row names the fields. When `false`, fields are named `column_0`, `column_1`, and so on."
        )
        @Builder.Default
        @PluginProperty(group = "advanced")
        private Property<Boolean> header = Property.ofValue(true);

        @Schema(title = "Character set")
        @Builder.Default
        @PluginProperty(group = "advanced")
        private Property<String> charset = Property.ofValue(StandardCharsets.UTF_8.name());

        @Schema(
            title = "Infer field types",
            description = "When `true`, blank fields become null and values that look like booleans, integers, or decimals are converted. When `false`, every field stays a string."
        )
        @Builder.Default
        @PluginProperty(group = "advanced")
        private Property<Boolean> inferTypes = Property.ofValue(true);
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "File URI containing the jq outputs")
        private final URI uri;

        @Schema(title = "Number of records written")
        private final Long processedItemsTotal;
    }
}
