package com.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

        properties.setProperty("group.id", "test11");
        properties.setProperty("auto.offset.reset", "latest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

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
}
