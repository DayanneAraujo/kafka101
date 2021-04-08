package one;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MyProducerDemoCallbackAndKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(MyProducerDemoCallbackAndKeys.class);
        String bootstrapServers = "127.0.0.1:9092";

        // Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        for (int i=0; i < 10; i++) {
            // Create a Producer Record
            String topic = "first_topic";
            String value = "hello world " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, value);

            // Note that same keys always goes to the same partitions
            logger.info("Key: " + key);

            // Send Data --> async (it'll run in the background)
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // Executes every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        logger.info("Received new metadata: \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp() + "\n"
                        );
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            }).get(); //block send() to be sync bad practice, just to see stuff - NOT FOR PRODUCTION!!!!!!
        }

        producer.flush();
        // flush and close producer
        producer.close();
    }
}
