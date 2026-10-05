package io.kestra.plugin.transform.arrow.reader;

import com.fasterxml.jackson.databind.JsonNode;
import io.kestra.plugin.transform.arrow.convert.AvroValueConverter;
import org.apache.avro.Conversions;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.LocalInputFile;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Reads Parquet rows through Avro generic records.
 * {@link LocalInputFile} plus {@link PlainParquetConfiguration} avoid opening a Hadoop filesystem.
 */
public final class ParquetRowReader implements RowReader {
    private final ParquetReader<GenericRecord> reader;

    public ParquetRowReader(Path path) throws IOException {
        GenericData model = genericData();
        this.reader = AvroParquetReader.<GenericRecord>builder(new LocalInputFile(path), new PlainParquetConfiguration())
            .withDataModel(model)
            .disableCompatibility()
            .build();
    }

    @Override
    public JsonNode next() throws IOException {
        GenericRecord record = reader.read();
        if (record == null) {
            return null;
        }
        return AvroValueConverter.toJson(record.getSchema(), record);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    private static GenericData genericData() {
        GenericData model = new GenericData();
        model.addLogicalTypeConversion(new Conversions.DecimalConversion());
        model.addLogicalTypeConversion(new Conversions.UUIDConversion());
        model.addLogicalTypeConversion(new TimeConversions.DateConversion());
        model.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());
        model.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
        model.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
        model.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
        model.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
        model.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());
        return model;
    }
}
