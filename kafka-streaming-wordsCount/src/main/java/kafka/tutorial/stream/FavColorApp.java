package kafka.tutorial.stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

public class FavColorApp {
  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "fav-color-app");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

    KStreamBuilder builder = new KStreamBuilder();

    KStream<String, String> textLines = builder.stream("fav-color-input");

    KStream<String, String> usersAndColor =
        textLines
            .filter((key, value) -> value.contains(","))
            .selectKey((key, value) -> value.split(",")[0].toLowerCase())
            .mapValues(value -> value.split(",")[1].toLowerCase())
            .filter((user, color) -> Arrays.asList("green", "blue", "red").contains(color));

    usersAndColor.to("user-keys-and-color");

    KTable<String, String> usersAndColorsTable = builder.table("user-keys-and-color");

    KTable<String, Long> favColor =
        usersAndColorsTable
            .groupBy((user, color) -> new KeyValue<>(color, color))
            .count("CountsByColor");

    favColor.to(Serdes.String(), Serdes.Long(), "fav-color-output");
    KafkaStreams streams = new KafkaStreams(builder, properties);
    streams.cleanUp();
    streams.start();
    System.out.println(streams.toString());
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }
}
