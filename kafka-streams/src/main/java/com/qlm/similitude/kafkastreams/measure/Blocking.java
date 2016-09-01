package com.qlm.similitude.kafkastreams.measure;

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
import java.util.stream.Collectors;

public class Blocking {
  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//    props.put("num.stream.threads", "4");

    KStreamBuilder builder = new KStreamBuilder();

    KStream<String, String> source = builder.stream("streams-file-input");
    StringSerializer stringSerializer = new StringSerializer();
    StringDeserializer stringDeserializer = new StringDeserializer();

    WindowedSerializer<String> windowedSerializer = new WindowedSerializer<>(stringSerializer);
    WindowedDeserializer<String> windowedDeserializer = new WindowedDeserializer<>(stringDeserializer);
    Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer, windowedDeserializer);
    source
      .flatMap(new BlockSentences(40, 4))
      .map((key, value)->new KeyValue<>(value, key.toString()))
      .reduceByKey(new CombineIds(), TimeWindows.of("streams-blocks-reduce", 20000).advanceBy(20000))
      .to(windowedSerde, Serdes.String(), "streams-blocks");

    final KStream<String, String> streamBlocks = builder.stream("streams-blocks");
    streamBlocks.flatMapValues(value->{
        String[] ids = value.split(",");
        Arrays.sort(ids);
        List<String> pairs = new ArrayList<>();
        for (int i = 0; i < ids.length; i++) {
          for (int j = i+1; j < ids.length; j++) {
            pairs.add(ids[i] + "\t" + ids[j]);
          }
        }
        return pairs;
      })
      .map((key, value)->new KeyValue<>(value, -1))
      .reduceByKey((value1, value2)->value1, TimeWindows.of("reduce-pairs", 40000).advanceBy(40000), Serdes.String(), Serdes.Integer())
      .to(windowedSerde, Serdes.Integer(), "block-pairs");

    KafkaStreams streams = new KafkaStreams(builder, props);
    streams.start();

    Thread.sleep(90000L);

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
      kvs.addAll(blocks.stream().map(block->new KeyValue<>(k, block)).collect(Collectors.toList()));
      return kvs;
    }
  }

  private static class CombineIds implements Reducer<String> {
    @Override public String apply(String v1, String v2) {
      return v1 + "," + v2;
    }
  }
}
