package com.github.kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FirstConsumerThread {

    private static final Logger log = LoggerFactory.getLogger(FirstConsumer.class);
    public static void main(String[] args) {
        String bootstrapServer = "127.0.0.1:9092";
        String topic = "first_topic";
        String groupId ="my_sixth_app";
        log.info("Starting my consumer ....");
        CountDownLatch latch = new CountDownLatch(1);
        ConsumerThread myConsumerThread =  new ConsumerThread(
                                bootstrapServer, 
                                groupId, 
                                topic, 
                                latch);

        Thread myThread =  new Thread(myConsumerThread);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            log.info("Caught shutdown hook");
            myConsumerThread.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                log.info("Application got interrupted !!!!");
            }finally{
                log.info("Application exit");
            }
            

        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            log.info("My consumer got interrupted !!!!");
        }finally{
            log.info("My consumer closed");
        }
    }

    public static class  ConsumerThread implements Runnable{
        private final Logger logger = LoggerFactory.getLogger(ConsumerThread.class);

        
        private CountDownLatch latch;
        private  KafkaConsumer<String,String> consumer;

        private ConsumerThread(String b, String groupId, String topic , CountDownLatch latch){  
            this.latch = latch;
            Properties properties = new Properties();
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, b);
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
            //create consumer
            consumer = new KafkaConsumer<>(properties);
            //subscribe
             consumer.subscribe(Collections.singleton(topic));
        }
        
        @Override
        public void run() {
             //poll data
             try {
                while(true){
                    ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String,String>  consumerRecord : records) {
                        log.info("Key : {} and Value : {}", consumerRecord.key(), consumerRecord.value());
                        log.info("Partition : {} and Offset : {}", consumerRecord.partition(),consumerRecord.offset());
                        
                    }
                 }
                
                 
             } catch (WakeupException e) {
                 logger.info("Received the shutdown signal !!!");
             }finally{
                 consumer.close();
                 latch.countDown();
             }
           
        }

       public void  shutdown(){
            consumer.wakeup();
        }
    }
    
}
