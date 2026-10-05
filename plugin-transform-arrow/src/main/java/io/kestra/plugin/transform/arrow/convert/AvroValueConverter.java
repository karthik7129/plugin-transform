package io.kestra.plugin.transform.arrow.convert;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.kestra.core.serializers.JacksonMapper;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Converts an Avro {@link GenericRecord} read from Parquet into a JSON object.
 * Logical dates and timestamps become ISO-8601 strings, matching the Arrow reader.
 */
public final class AvroValueConverter {
    private static final ObjectMapper JSON = JacksonMapper.ofJson();
    private static final Conversions.DecimalConversion DECIMALS = new Conversions.DecimalConversion();

    private AvroValueConverter() {
    }

    public static JsonNode toJson(Schema schema, Object value) {
        if (value == null) {
            return NullNode.getInstance();
        }
        if (value instanceof GenericRecord record) {
            return record(record);
        }

        if (schema != null && schema.getType() == Schema.Type.UNION) {
            return union(schema, value);
        }

        JsonNode logical = logical(schema, value);
        if (logical != null) {
            return logical;
        }

        if (schema != null && schema.getType() == Schema.Type.ARRAY && value instanceof List<?> list) {
            ArrayNode array = JSON.createArrayNode();
            Schema element = schema.getElementType();
            for (Object item : list) {
                array.add(toJson(element, item));
            }
            return array;
        }
        if (schema != null && schema.getType() == Schema.Type.MAP && value instanceof Map<?, ?> map) {
            ObjectNode node = JSON.createObjectNode();
            Schema element = schema.getValueType();
            map.forEach((key, item) -> node.set(String.valueOf(key), toJson(element, item)));
            return node;
        }
        if (schema != null && (schema.getType() == Schema.Type.BYTES || schema.getType() == Schema.Type.FIXED)) {
            return binary(value);
        }
        if (schema != null && (schema.getType() == Schema.Type.STRING || schema.getType() == Schema.Type.ENUM)) {
            return TextNode.valueOf(String.valueOf(value));
        }
        return javaValue(value);
    }

    private static ObjectNode record(GenericRecord record) {
        ObjectNode node = JSON.createObjectNode();
        for (Schema.Field field : record.getSchema().getFields()) {
            node.set(field.name(), toJson(field.schema(), record.get(field.name())));
        }
        return node;
    }

    private static JsonNode union(Schema schema, Object value) {
        for (Schema branch : schema.getTypes()) {
            if (branch.getType() == Schema.Type.NULL) {
                continue;
            }
            if (matches(branch, value)) {
                return toJson(branch, value);
            }
        }
        return javaValue(value);
    }

    private static boolean matches(Schema branch, Object value) {
        return switch (branch.getType()) {
            case RECORD -> value instanceof GenericRecord;
            case ARRAY -> value instanceof List<?>;
            case MAP -> value instanceof Map<?, ?>;
            case STRING, ENUM -> value instanceof CharSequence;
            case BYTES, FIXED -> value instanceof ByteBuffer || value instanceof byte[];
            case INT -> value instanceof Integer;
            case LONG -> value instanceof Long;
            case FLOAT -> value instanceof Float;
            case DOUBLE -> value instanceof Double;
            case BOOLEAN -> value instanceof Boolean;
            case NULL -> value == null;
            case UNION -> true;
        };
    }

    private static JsonNode logical(Schema schema, Object value) {
        if (schema == null || schema.getLogicalType() == null || value == null) {
            return null;
        }
        LogicalType logicalType = schema.getLogicalType();
        String name = logicalType.getName();
        if ("date".equals(name) && value instanceof Number number) {
            return TextNode.valueOf(LocalDate.ofEpochDay(number.longValue()).toString());
        }
        if ("time-millis".equals(name) && value instanceof Number number) {
            return TextNode.valueOf(LocalTime.ofNanoOfDay(number.longValue() * 1_000_000L).toString());
        }
        if ("time-micros".equals(name) && value instanceof Number number) {
            return TextNode.valueOf(LocalTime.ofNanoOfDay(number.longValue() * 1_000L).toString());
        }
        if ("timestamp-millis".equals(name) && value instanceof Number number) {
            return TextNode.valueOf(Instant.ofEpochMilli(number.longValue()).toString());
        }
        if ("timestamp-micros".equals(name) && value instanceof Number number) {
            long micros = number.longValue();
            return TextNode.valueOf(Instant.ofEpochSecond(Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1_000L).toString());
        }
        if ("local-timestamp-millis".equals(name) && value instanceof Number number) {
            return TextNode.valueOf(LocalDateTime.ofInstant(Instant.ofEpochMilli(number.longValue()), java.time.ZoneOffset.UTC).toString());
        }
        if ("local-timestamp-micros".equals(name) && value instanceof Number number) {
            long micros = number.longValue();
            Instant instant = Instant.ofEpochSecond(Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1_000L);
            return TextNode.valueOf(LocalDateTime.ofInstant(instant, java.time.ZoneOffset.UTC).toString());
        }
        if (logicalType instanceof LogicalTypes.Decimal && value instanceof ByteBuffer buffer) {
            BigDecimal decimal = DECIMALS.fromBytes(buffer, schema, logicalType);
            return DecimalNode.valueOf(decimal);
        }
        if (value instanceof LocalDate || value instanceof LocalTime || value instanceof LocalDateTime
            || value instanceof Instant || value instanceof OffsetDateTime || value instanceof ZonedDateTime) {
            return TextNode.valueOf(value.toString());
        }
        return null;
    }

    private static JsonNode binary(Object value) {
        byte[] bytes;
        if (value instanceof byte[] array) {
            bytes = array;
        } else if (value instanceof ByteBuffer buffer) {
            ByteBuffer duplicate = buffer.duplicate();
            bytes = new byte[duplicate.remaining()];
            duplicate.get(bytes);
        } else {
            return TextNode.valueOf(String.valueOf(value));
        }
        return TextNode.valueOf(Base64.getEncoder().encodeToString(bytes));
    }

    private static JsonNode javaValue(Object value) {
        if (value instanceof LocalDate || value instanceof LocalTime || value instanceof LocalDateTime
            || value instanceof Instant || value instanceof OffsetDateTime || value instanceof ZonedDateTime) {
            return TextNode.valueOf(value.toString());
        }
        if (value instanceof ByteBuffer || value instanceof byte[]) {
            return binary(value);
        }
        if (value instanceof CharSequence) {
            return TextNode.valueOf(value.toString());
        }
        return JSON.valueToTree(value);
    }
}
