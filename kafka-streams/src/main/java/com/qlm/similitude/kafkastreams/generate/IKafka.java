package com.qlm.similitude.kafkastreams.generate;

import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

@SuppressWarnings("WeakerAccess")
public interface IKafka <K, V>{
  Future<RecordMetadata> sendMessage(K key,V message);
}
