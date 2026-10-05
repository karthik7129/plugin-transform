package io.kestra.plugin.transform.arrow.convert;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ArrowValueConverterTest {
    @Test
    void dictionaryIndexesAreDecodedToValues() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
             VarCharVector dictionaryValues = new VarCharVector("dict", allocator)) {
            dictionaryValues.allocateNew();
            dictionaryValues.setSafe(0, "red".getBytes(StandardCharsets.UTF_8));
            dictionaryValues.setSafe(1, "blue".getBytes(StandardCharsets.UTF_8));
            dictionaryValues.setValueCount(2);

            DictionaryEncoding encoding = new DictionaryEncoding(7L, false, new ArrowType.Int(32, true));
            Dictionary dictionary = new Dictionary(dictionaryValues, encoding);

            Field field = new Field("color", new FieldType(true, new ArrowType.Int(32, true), encoding), null);
            try (FieldVector indexes = field.createVector(allocator)) {
                IntVector ints = (IntVector) indexes;
                ints.allocateNew();
                ints.set(0, 1);
                ints.set(1, 0);
                ints.setNull(2);
                ints.setValueCount(3);

                Map<Long, org.apache.arrow.vector.dictionary.Dictionary> dictionaries = Map.of(7L, dictionary);
                assertEquals("blue", ArrowValueConverter.toJson(indexes, 0, dictionaries).asText());
                assertEquals("red", ArrowValueConverter.toJson(indexes, 1, dictionaries).asText());
                JsonNode missing = ArrowValueConverter.toJson(indexes, 2, dictionaries);
                assertEquals(true, missing.isNull());
            }
        }
    }
}
