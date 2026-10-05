package io.kestra.plugin.transform.arrow.convert;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BigIntegerNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.kestra.core.serializers.JacksonMapper;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Converts one Arrow row into a JSON object.
 * <p>
 * Temporal values are ISO-8601 strings rather than epoch numbers, so Kestra outputs stay readable.
 * Map columns follow {@code aq} and become arrays of {@code {key, value}} objects.
 * Unsigned 64-bit integers are not narrowed to a signed long.
 */
public final class ArrowValueConverter {
    private static final ObjectMapper JSON = JacksonMapper.ofJson();

    private ArrowValueConverter() {
    }

    public static ObjectNode toRow(VectorSchemaRoot root, int index, Map<Long, Dictionary> dictionaries) {
        ObjectNode row = JSON.createObjectNode();
        for (Field field : root.getSchema().getFields()) {
            row.set(field.getName(), toJson(root.getVector(field.getName()), index, dictionaries));
        }
        return row;
    }

    public static JsonNode toJson(FieldVector vector, int index, Map<Long, Dictionary> dictionaries) {
        if (vector == null || vector.isNull(index)) {
            return NullNode.getInstance();
        }

        DictionaryEncoding encoding = vector.getField().getDictionary();
        if (encoding != null) {
            Dictionary dictionary = dictionaries == null ? null : dictionaries.get(encoding.getId());
            if (dictionary == null) {
                throw new IllegalArgumentException(
                    "Missing Arrow dictionary " + encoding.getId() + " for field '" + vector.getField().getName() + "'"
                );
            }
            Object dictionaryIndex = vector.getObject(index);
            if (!(dictionaryIndex instanceof Number number)) {
                throw new IllegalArgumentException(
                    "Dictionary index for field '" + vector.getField().getName() + "' is not a number"
                );
            }
            return toJson(dictionary.getVector(), number.intValue(), dictionaries);
        }

        if (vector instanceof MapVector mapVector) {
            return map(mapVector, index, dictionaries);
        }
        if (vector instanceof FixedSizeListVector fixedList) {
            return fixedList(fixedList, index, dictionaries);
        }
        if (vector instanceof ListVector listVector) {
            return list(listVector, index, dictionaries);
        }
        if (vector instanceof StructVector structVector) {
            return struct(structVector, index, dictionaries);
        }

        ArrowType type = vector.getField().getType();
        if (type instanceof ArrowType.Int intType && !intType.getIsSigned() && intType.getBitWidth() == 64) {
            return unsignedLong(vector, index);
        }
        if (type instanceof ArrowType.Date date && date.getUnit() == DateUnit.DAY) {
            Object raw = vector.getObject(index);
            if (raw instanceof Number number) {
                return TextNode.valueOf(LocalDate.ofEpochDay(number.longValue()).toString());
            }
        }
        if (type instanceof ArrowType.Timestamp timestamp
            && timestamp.getTimezone() != null
            && !timestamp.getTimezone().isEmpty()) {
            Object raw = vector.getObject(index);
            if (raw instanceof Number number) {
                return TextNode.valueOf(zonedTimestamp(timestamp, number.longValue()));
            }
        }
        if (type instanceof ArrowType.FloatingPoint floating
            && floating.getPrecision() == FloatingPointPrecision.HALF) {
            Object raw = vector.getObject(index);
            if (raw instanceof Number number) {
                float value = Float.float16ToFloat(number.shortValue());
                return Float.isFinite(value) ? FloatNode.valueOf(value) : NullNode.getInstance();
            }
        }

        return normalize(vector.getObject(index));
    }

    /**
     * Arrow stores uint64 in {@link UInt8Vector}. {@code get(int)} is the raw two's-complement bits,
     * which is negative for values above {@link Long#MAX_VALUE}, so the JSON value is an unsigned integer.
     */
    private static JsonNode unsignedLong(FieldVector vector, int index) {
        if (vector instanceof UInt8Vector uint8) {
            BigInteger value = uint8.getObjectNoOverflow(index);
            return value == null ? NullNode.getInstance() : new BigIntegerNode(value);
        }
        if (vector instanceof BigIntVector bigInt) {
            return new BigIntegerNode(new BigInteger(Long.toUnsignedString(bigInt.get(index))));
        }
        Object raw = vector.getObject(index);
        if (raw instanceof BigInteger big) {
            return new BigIntegerNode(big);
        }
        if (raw instanceof Number number) {
            return new BigIntegerNode(new BigInteger(Long.toUnsignedString(number.longValue())));
        }
        throw new IllegalArgumentException(
            "Unsupported unsigned 64-bit vector " + vector.getClass().getName() + " for field '" + vector.getField().getName() + "'"
        );
    }

    private static String zonedTimestamp(ArrowType.Timestamp timestamp, long value) {
        Instant instant = switch (timestamp.getUnit()) {
            case SECOND -> Instant.ofEpochSecond(value);
            case MILLISECOND -> Instant.ofEpochMilli(value);
            case MICROSECOND -> Instant.ofEpochSecond(Math.floorDiv(value, 1_000_000L), Math.floorMod(value, 1_000_000L) * 1_000L);
            case NANOSECOND -> Instant.ofEpochSecond(Math.floorDiv(value, 1_000_000_000L), Math.floorMod(value, 1_000_000_000L));
        };
        return instant.atZone(ZoneId.of(timestamp.getTimezone())).toString();
    }

    private static ArrayNode map(MapVector vector, int index, Map<Long, Dictionary> dictionaries) {
        StructVector entries = (StructVector) vector.getDataVector();
        FieldVector keys = entries.getChild(MapVector.KEY_NAME);
        FieldVector values = entries.getChild(MapVector.VALUE_NAME);
        int start = vector.getElementStartIndex(index);
        int end = vector.getElementEndIndex(index);
        ArrayNode array = JSON.createArrayNode();
        for (int i = start; i < end; i++) {
            ObjectNode entry = JSON.createObjectNode();
            entry.set("key", toJson(keys, i, dictionaries));
            entry.set("value", toJson(values, i, dictionaries));
            array.add(entry);
        }
        return array;
    }

    private static ArrayNode list(ListVector vector, int index, Map<Long, Dictionary> dictionaries) {
        FieldVector data = vector.getDataVector();
        int start = vector.getElementStartIndex(index);
        int end = vector.getElementEndIndex(index);
        ArrayNode array = JSON.createArrayNode();
        for (int i = start; i < end; i++) {
            array.add(toJson(data, i, dictionaries));
        }
        return array;
    }

    private static ArrayNode fixedList(FixedSizeListVector vector, int index, Map<Long, Dictionary> dictionaries) {
        FieldVector data = vector.getDataVector();
        int size = vector.getListSize();
        int start = index * size;
        ArrayNode array = JSON.createArrayNode();
        for (int i = 0; i < size; i++) {
            array.add(toJson(data, start + i, dictionaries));
        }
        return array;
    }

    private static ObjectNode struct(StructVector vector, int index, Map<Long, Dictionary> dictionaries) {
        ObjectNode node = JSON.createObjectNode();
        for (String name : vector.getChildFieldNames()) {
            node.set(name, toJson(vector.getChild(name), index, dictionaries));
        }
        return node;
    }

    private static JsonNode normalize(Object value) {
        if (value == null) {
            return NullNode.getInstance();
        }
        if (value instanceof JsonNode node) {
            return node;
        }
        if (value instanceof Map<?, ?> map) {
            ObjectNode node = JSON.createObjectNode();
            map.forEach((key, item) -> node.set(String.valueOf(key), normalize(item)));
            return node;
        }
        if (value instanceof List<?> list) {
            ArrayNode array = JSON.createArrayNode();
            for (Object item : list) {
                array.add(normalize(item));
            }
            return array;
        }
        if (value instanceof byte[] bytes) {
            return TextNode.valueOf(Base64.getEncoder().encodeToString(bytes));
        }
        if (value instanceof ByteBuffer buffer) {
            ByteBuffer duplicate = buffer.duplicate();
            byte[] bytes = new byte[duplicate.remaining()];
            duplicate.get(bytes);
            return TextNode.valueOf(Base64.getEncoder().encodeToString(bytes));
        }
        if (value instanceof LocalDate || value instanceof LocalDateTime || value instanceof LocalTime
            || value instanceof Instant || value instanceof OffsetDateTime || value instanceof ZonedDateTime
            || value instanceof Duration || value instanceof Period) {
            return TextNode.valueOf(value.toString());
        }
        if (value instanceof Float number && !Float.isFinite(number)) {
            return NullNode.getInstance();
        }
        if (value instanceof Double number && !Double.isFinite(number)) {
            return NullNode.getInstance();
        }
        if (value instanceof BigInteger number) {
            return new BigIntegerNode(number);
        }
        if (value instanceof BigDecimal number) {
            return DecimalNode.valueOf(number);
        }
        try {
            return JSON.valueToTree(value);
        } catch (IllegalArgumentException e) {
            return TextNode.valueOf(String.valueOf(value));
        }
    }
}
