package one;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class MyConsumerDemoGroupsThreads {
    public static void main(String[] args) {
        new MyConsumerDemoGroupsThreads().run();
    }

    private MyConsumerDemoGroupsThreads() {}

    private void run() {
        Logger logger = LoggerFactory.getLogger(MyConsumerDemoGroupsThreads.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-sixth-application";
        String topic = "first_topic";

        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        logger.info("Creating consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(
                bootstrapServers,
                groupId,
                topic,
                latch
        );

        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // shutdown application
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            // Await until application is over
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application interrupted", e);
        } finally {
            logger.info("Application closing");
        }
    }

    public class ConsumerRunnable implements Runnable {
        private final CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());


        public ConsumerRunnable(String bootstrapServers,
                                String groupId,
                                String topic,
                                CountDownLatch latch) {

            this.latch = latch;

            // Create Consumer Configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // Create consumer
            consumer = new KafkaConsumer<String, String>(properties);

            // Subscribe to topic
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {
                // poll for new data
                while (true) {
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Shutdown signal");
            } finally {
                consumer.close();
                // tell our main code we're done with consumer
                latch.countDown();
            }
        }

        public void shutdown(){
            // Method to interrupt consumer.poll()
            // It'll throw the exception WakeUpException
            consumer.wakeup();
        }
    }
}
