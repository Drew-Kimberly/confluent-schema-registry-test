package org.example;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

public class Main {
    public static void main(String[] args) throws IOException {
        var topic = "avro-poc-entityA";

        var rawAvroSchema = "{\"type\":\"record\",\"name\":\"MyClass\",\"namespace\":\"com.test.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}";
        var eventData = "{\"id\":\"123\",\"extra\":999}";
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
        byte[] avroBytes = serializer.serialize(topic, avroRecord);

        System.out.println("Encoded bytes: " + Arrays.toString(avroBytes));

        var deserializer = new KafkaAvroDeserializer();
        deserializer.configure(config, false);
        var deserialized = deserializer.deserialize(topic, avroBytes);

        System.out.println("Deserialized Message: " + deserialized.toString());
    }
}