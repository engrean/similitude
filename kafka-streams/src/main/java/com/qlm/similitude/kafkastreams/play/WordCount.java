package com.qlm.similitude.kafkastreams.play;

import com.qlm.similitude.lsh.LshBlocking;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;

import java.util.*;

public class WordCount {
  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    KStreamBuilder builder = new KStreamBuilder();

    KStream<String, String> source = builder.stream("streams-file-input");
    StringSerializer stringSerializer = new StringSerializer();
    StringDeserializer stringDeserializer = new StringDeserializer();

    WindowedSerializer<String> windowedSerializer = new WindowedSerializer<>(stringSerializer);
    WindowedDeserializer<String> windowedDeserializer = new WindowedDeserializer<>(stringDeserializer);
    Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer, windowedDeserializer);
    final KTable<Windowed<String>, String> blockKeys = source
      .flatMap(new BlockSentences(20, 4))
      .map((key, value)->new KeyValue<>(value, key.toString()))
      .reduceByKey(new CombineIds(), TimeWindows.of("reduce-blocks", 20000).advanceBy(20000))
      .filter((key, value)->value.split(",").length > 1);
    blockKeys.to(windowedSerde, Serdes.String(), "streams-blocks");

    KafkaStreams streams = new KafkaStreams(builder, props);
    streams.start();

    Thread.sleep(50000L);

    streams.close();
  }

  private static class BlockSentences implements KeyValueMapper<String, String, Iterable<KeyValue<Long, String>>> {
    private final LshBlocking blocking;

    private BlockSentences(int numHashFuntions, int numBands) {
      blocking  = new LshBlocking(numHashFuntions, numBands);
    }

    @Override
    public Iterable<KeyValue<Long, String>> apply(String key, String value){
      final Set<String> blocks = blocking.lsh(value.toLowerCase(Locale.getDefault()).split(" "));
      List<KeyValue<Long, String>> kvs = new ArrayList<>();
      Long k = Long.parseLong(key);
      for (String block: blocks) {
        kvs.add(new KeyValue<>(k, block));
      }
      return kvs;
    }
  }

  private static class CombineIds implements Reducer<String> {
    @Override public String apply(String v1, String v2) {
      return v1 + "," + v2;
    }
  }
}
