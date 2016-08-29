package com.qlm.similitude.kafkastreams.generate;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

@SuppressWarnings("WeakerAccess")
public class KafkaClient implements IKafka<Integer, String> {

  private final static Schema schema = SchemaBuilder.record("sentences").fields().requiredString("line").endRecord();

  private KafkaProducer<Integer, GenericData.Record> producer;
  private String topic;

  public KafkaClient(String seedIp, String topic) {
    this.topic = topic;
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, seedIp+":9092");
    properties.put("auto.offset.reset", "earliest");
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "groovyConsumerGroup"+topic);
    properties.put("schema.registry.url", "http://"+ seedIp+":8081");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    producer = new KafkaProducer<>(properties);
  }

  public Future<RecordMetadata> sendMessage(Integer key, String message) {
    GenericData.Record msg = new GenericData.Record(getSchema());
    msg.put("line", message);
    return producer.send(new ProducerRecord<>(topic, key, msg));
  }

  private static Schema getSchema(){
    return schema;
  }
}
