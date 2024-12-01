package org.example;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.debezium.transforms.outbox.EventRouter;
import io.debezium.transforms.outbox.JsonSchemaData;
import org.apache.avro.Schema;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

public class Main {
    public static void main(String[] args) throws IOException {
        var topic = "avro-poc-entityA";

        var rawAvroSchema = "{\"type\":\"record\",\"name\":\"MyClass\",\"namespace\":\"com.test.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}";
        var eventData = "{\"foo\":\"123\"}";
        var avroSchemaParser = new Schema.Parser();
        var avroSchema = avroSchemaParser.parse(rawAvroSchema);

        var config = new HashMap<String, String>();
        config.put("schema.registry.url", "http://localhost:8081");
        config.put("auto.register.schemas", "false");
        config.put("use.latest.version", "true");
        config.put("latest.compatibility.strict", "false");
        config.put("subject.name.strategy", "TopicNameStrategy");

        var serializer = new KafkaAvroSerializer();
        serializer.configure(config, false);

        var avroRecord = AvroUtils.jsonToGenericRecord(eventData, avroSchema);
//        byte[] avroBytes = serializer.serialize(topic, avroRecord);

//        System.out.println("Encoded bytes: " + Arrays.toString(avroBytes));
//
        var deserializer = new KafkaAvroDeserializer();
        deserializer.configure(config, false);
//        var deserialized = deserializer.deserialize(topic, avroBytes);

//        System.out.println("Deserialized Message: " + deserialized.toString());

        var debezium = new JsonSchemaData();
        var json = AvroUtils.readJson(eventData);
//        var payloadSchema = debezium.toConnectSchema("event", json);
//        var payload = debezium.toConnectData(json, payloadSchema);
        var payloadSchema = SchemaBuilder.struct()
            .name("MySchema")
            .field("foo", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .field("id", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .build();
        var payload = new Struct(payloadSchema)
                .put("foo", "123")
                .put("id", "456");
//        var payload = new Struct
        System.out.println("Connect Schema from Debezium: " + ToStringBuilder.reflectionToString(payloadSchema));
        System.out.println("Connect Data from Debezium: " + ToStringBuilder.reflectionToString(payload));

        var converter = new AvroConverter();
        converter.configure(config, false);

        var converted = converter.fromConnectData(topic, payloadSchema, payload);
        var deserialized = deserializer.deserialize(topic, converted);

        System.out.println("Encoded (Converter): " + converted.toString());
        System.out.println("Deserialized (Converter): " + deserialized.toString());
    }
}