package com.github.kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FirstConsumer {

    private static final Logger log = LoggerFactory.getLogger(FirstConsumer.class);
    public static void main(String[] args) {
        String bootstrapServer = "127.0.0.1:9092";
        String topic = "first_topic";
        String groupId ="my_fifh_app";
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //create consumer

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);
        //subscribe
        consumer.subscribe(Collections.singleton(topic));
        //poll data
        while(true){
          ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
          for (ConsumerRecord<String,String>  consumerRecord : records) {
              log.info("Key : {} and Value : {}", consumerRecord.key(), consumerRecord.value());
              log.info("Partition : {} and Offset : {}", consumerRecord.partition(),consumerRecord.offset());
              
          }
        }
    }
    
}
