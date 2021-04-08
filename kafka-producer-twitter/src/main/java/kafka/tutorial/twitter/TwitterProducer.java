package kafka.tutorial.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

  Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

  String consumerKey = "FTgDGUSVOB7yBXD4XqFjhmS91";
  String consumerSecret = "T58mBPjpdoTQZsZVAkJ4P7cMUGXcZ7FTQwEApgXgV1PeNg9ws3";
  String token = "1377233273953288198-2WJcUZg106WaC7bmlMDGeK1JmPrLHA";
  String secret = "F6FtAH80nXHt5MXpbupUKm87YoTehq6kjsUnf93A9BfJp";

  public TwitterProducer() {}

  public static void main(String[] args) {
    new TwitterProducer().run();
  }

  public void run() {
    BlockingQueue<String> msgQueue = new LinkedBlockingDeque<>(100000);
    // Create Twitter client
    Client client = createTwitterClient(msgQueue);
    client.connect();

    // Create kafka producer
    KafkaProducer<String, String> producer = createKafkaProducer();

    // Add a shutdown hook
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  logger.info("Stopping application");
                  logger.info("Shutting down client from twitter");
                  client.stop();
                  logger.info("Closing producer");
                  producer.close();
                  logger.info("done!!");
                }));

    // Loop to send tweets to kafka
    while (!client.isDone()) {
      String msg = null;
      try {
        msg = msgQueue.poll(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
        client.stop();
      }
      if (msg != null) {
        logger.info(msg);
        producer.send(
            new ProducerRecord<>("twitter_tweets", null, msg),
            new Callback() {
              @Override
              public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) logger.error("Something bad happened", exception);
              }
            });
      }
    }
    logger.info("End of application");
  }

  private KafkaProducer<String, String> createKafkaProducer() {
    String bootstrapServers = "127.0.0.1:9092";

    // Create Producer Properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // Create safe props
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
    properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

    // high throughput settings
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(64 * 1024));

    // Create Producer
    KafkaProducer<String, String> producer = new KafkaProducer(properties);
    return producer;
  }

  private Client createTwitterClient(BlockingQueue<String> msgQueue) {

    Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
    StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
    List<String> terms = Lists.newArrayList("Kafka","Blockchain");
    hosebirdEndpoint.trackTerms(terms);
    Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

    ClientBuilder builder =
        new ClientBuilder()
            .name("Hosebird-Client-01")
            .hosts(hosebirdHosts)
            .authentication(hosebirdAuth)
            .endpoint(hosebirdEndpoint)
            .processor(new StringDelimitedProcessor(msgQueue));
    return builder.build();
  }
}
