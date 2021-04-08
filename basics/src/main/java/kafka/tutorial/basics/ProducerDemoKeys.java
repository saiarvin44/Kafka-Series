package kafka.tutorial.basics;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
  public static void main(String[] args) throws ExecutionException, InterruptedException {

    Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

    String bootstrapServers = "127.0.0.1:9092";

    // Create Producer Properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // Create Producer
    KafkaProducer<String, String> producer = new KafkaProducer(properties);

    // Create Producer Record
    ProducerRecord<String, String> record =
        new ProducerRecord<>("first_topic", "FirstKey","Hello World Again With Key");

    // Send data - asynchronous
    producer.send(
        record,
        new Callback() {
          @Override
          public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception == null) {
              logger.info(
                  "Received new metadata : \n"
                      + "Topic : "
                      + metadata.topic()
                      + "\n"
                      + "Partition : "
                      + metadata.partition()
                      + "\n"
                      + "Offset : "
                      + metadata.offset());
            } else logger.error("Error while producing : " + exception);
          }
        }).get(); // Make it synchronous (not recommended)

    // flush data
    producer.flush();

    // flushes and close producer
    producer.close();
  }
}
