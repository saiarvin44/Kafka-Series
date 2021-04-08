package kafka.tutorial.ELKConsumer;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ELKConsumer {
  private static final Logger logger = LoggerFactory.getLogger(ELKConsumer.class.getName());

  public static RestHighLevelClient createClient() {
    String hostname = "kafka-tutorial-4692280483.us-east-1.bonsaisearch.net";
    String username = "";
    String password = "";

    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(
        AuthScope.ANY, new UsernamePasswordCredentials(username, password));

    RestClientBuilder builder =
        RestClient.builder(new HttpHost(hostname, 443, "https"))
            .setHttpClientConfigCallback(
                new RestClientBuilder.HttpClientConfigCallback() {
                  @Override
                  public HttpAsyncClientBuilder customizeHttpClient(
                      HttpAsyncClientBuilder httpClientBuilder) {
                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                  }
                });

    RestHighLevelClient client = new RestHighLevelClient(builder);
    return client;
  }

  public static KafkaConsumer<String, String> kafkaConsumer(String topic) {
    String bootstrapServers = "127.0.0.1:9092";
    String groupId = "kafka-demo-ELK";

    // Create Consumer Configs
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // diable auto commit of offsets
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20");

    // Create Consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);
    consumer.subscribe(Arrays.asList(topic));
    return consumer;
  }

  public static void main(String[] args) throws IOException {

    RestHighLevelClient client = createClient();

    KafkaConsumer<String, String> consumer = kafkaConsumer("twitter_tweets");

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      Integer recordsCount = records.count();
      logger.info("Received " + recordsCount + " records");

      BulkRequest bulkRequest = new BulkRequest();

      for (ConsumerRecord<String, String> record : records) {

        // Kafka generic ID
        String id = record.topic() + "_" + record.partition() + "_" + record.offset();

        IndexRequest indexRequest =
            new IndexRequest("twitter", "tweets", id).source(record.value(), XContentType.JSON);

        bulkRequest.add(indexRequest); // add to our bulk request
      }
      if (recordsCount > 0) {
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

        logger.info("Committing offsets");
        consumer.commitSync();
        logger.info("Offsets have been committed");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
    // client.close();
  }
}
