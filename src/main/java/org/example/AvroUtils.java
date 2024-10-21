package org.example;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AvroUtils {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static GenericRecord jsonToGenericRecord(String json, Schema schema) throws IOException {
        Map<String, Object> jsonMap = objectMapper.readValue(json, Map.class);
        GenericRecord record = new GenericData.Record(schema);
        populateRecord(record, jsonMap, schema);
        return record;
    }

    private static void populateRecord(GenericRecord record, Map<String, Object> jsonMap, Schema schema) {
        for (Schema.Field field : schema.getFields()) {
            String fieldName = field.name();
            if (jsonMap.containsKey(fieldName)) {
                Object value = jsonMap.get(fieldName);
                if (field.schema().getType() == Schema.Type.RECORD) {
                    // Nested record
                    GenericRecord nestedRecord = new GenericData.Record(field.schema());
                    populateRecord(nestedRecord, (Map<String, Object>) value, field.schema());
                    record.put(fieldName, nestedRecord);
                } else if (field.schema().getType() == Schema.Type.ARRAY) {
                    // Handle arrays
                    List<Map<String, Object>> list = (List<Map<String, Object>>) value;
                    GenericData.Array<GenericRecord> avroArray = new GenericData.Array(field.schema(), new ArrayList());
                    for (Map<String, Object> item : list) {
                        GenericRecord arrayRecord = new GenericData.Record(field.schema().getElementType());
                        populateRecord(arrayRecord, item, field.schema().getElementType());
                        avroArray.add(arrayRecord);
                    }
                    record.put(fieldName, avroArray);
                } else {
                    // Directly put other types
                    record.put(fieldName, value);
                }
            }
        }
    }
}
