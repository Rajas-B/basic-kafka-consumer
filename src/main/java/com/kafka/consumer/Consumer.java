package com.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {

    private static final Logger log = LoggerFactory.getLogger(Consumer.class.getSimpleName());

    public static void main(String[] args) {
        log.info("This is a Kafka Consumer");

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "");
        properties.setProperty("security.protocol", "PLAINTEXT");

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", "");
        properties.setProperty("auto.offset.reset", "latest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get a reference to the main thread

        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown. Exit by calling consumer.wakeup()");
                consumer.wakeup();

                // Join the main thread to allow execution code in main thread

                try{
                    mainThread.join();
                }
                catch(InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try{
            consumer.subscribe(Arrays.asList("Atest"));

            while(true) {
//            System.out.println("Polling");
                log.info("Polling");

                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));

                for(ConsumerRecord<String, String> record: records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: ", record.partition()+ ", Offset: " + record.offset());
                }

            }
        }
        catch(WakeupException e){
            log.info("Consumer is shutting down");
        }
        catch(Exception e){
            log.error("Unexpected exception in the consumer", e);
        }
        finally{
            consumer.close();
            log.info("The consumer has now gracefully shutdown");
        }
    }
}
