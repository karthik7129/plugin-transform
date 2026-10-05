package io.kestra.plugin.transform.arrow.reader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRecord;
import de.siegmar.fastcsv.reader.NamedCsvRecord;
import io.kestra.core.serializers.JacksonMapper;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public final class CsvRowReader implements RowReader {
    private static final ObjectMapper JSON = JacksonMapper.ofJson();
    private static final Pattern INTEGER = Pattern.compile("-?\\d+");
    private static final Pattern DECIMAL = Pattern.compile("-?(?:\\d+\\.\\d+|\\d+[eE][+-]?\\d+|\\d+\\.\\d+[eE][+-]?\\d+)");

    private final Reader reader;
    private final CsvReader<?> csv;
    private final Iterator<?> rows;
    private final boolean inferTypes;
    private List<String> headerNames;

    public CsvRowReader(Path path, CsvConfig config) throws IOException {
        this.reader = new InputStreamReader(Files.newInputStream(path), config.charset());
        this.inferTypes = config.inferTypes();
        CsvReader.CsvReaderBuilder builder = CsvReader.builder().fieldSeparator(config.delimiter());
        if (config.header()) {
            CsvReader<NamedCsvRecord> named = builder.ofNamedCsvRecord(this.reader);
            this.csv = named;
            this.rows = named.iterator();
        } else {
            CsvReader<CsvRecord> plain = builder.ofCsvRecord(this.reader);
            this.csv = plain;
            this.rows = plain.iterator();
        }
    }

    @Override
    public JsonNode next() {
        if (!rows.hasNext()) {
            return null;
        }
        Object next = rows.next();
        if (next instanceof NamedCsvRecord named) {
            return namedRow(named);
        }
        return plainRow((CsvRecord) next);
    }

    private ObjectNode namedRow(NamedCsvRecord record) {
        if (headerNames == null) {
            headerNames = record.getHeader();
        }
        ObjectNode node = JSON.createObjectNode();
        List<String> fields = record.getFields();
        for (int i = 0; i < headerNames.size(); i++) {
            String name = headerName(headerNames.get(i), i);
            String raw = i < fields.size() ? fields.get(i) : null;
            node.set(name, value(raw));
        }
        return node;
    }

    private ObjectNode plainRow(CsvRecord record) {
        ObjectNode node = JSON.createObjectNode();
        List<String> fields = record.getFields();
        for (int i = 0; i < fields.size(); i++) {
            node.set("column_" + i, value(fields.get(i)));
        }
        return node;
    }

    private static String headerName(String name, int index) {
        if (name == null || name.isBlank()) {
            return "column_" + index;
        }
        return name;
    }

    private JsonNode value(String raw) {
        if (raw == null) {
            return NullNode.getInstance();
        }
        if (!inferTypes) {
            return TextNode.valueOf(raw);
        }
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            return NullNode.getInstance();
        }
        if ("true".equalsIgnoreCase(trimmed) || "false".equalsIgnoreCase(trimmed)) {
            return BooleanNode.valueOf(Boolean.parseBoolean(trimmed));
        }
        if (INTEGER.matcher(trimmed).matches()) {
            try {
                return LongNode.valueOf(Long.parseLong(trimmed));
            } catch (NumberFormatException e) {
                return JSON.getNodeFactory().numberNode(new BigInteger(trimmed));
            }
        }
        if (DECIMAL.matcher(trimmed).matches()) {
            return DecimalNode.valueOf(new BigDecimal(trimmed));
        }
        return TextNode.valueOf(raw);
    }

    @Override
    public void close() throws IOException {
        try {
            csv.close();
        } finally {
            reader.close();
        }
    }
}
