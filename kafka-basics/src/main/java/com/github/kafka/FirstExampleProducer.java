package com.github.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class FirstExampleProducer 
{

    static final Logger log =  LoggerFactory.getLogger(FirstExampleProducer.class);
    public static void main( String[] args )
    {
        //Create producer properties
        Properties properties =  new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String,String> kafkaProducer =  new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String,String> record = new ProducerRecord<String,String>("first-topic", "Hello world number : "+i);
            kafkaProducer.send(record,(recordMetada, exception) ->{
                if(exception ==  null){
                    log.info(" receiving metadata :  \n Topic : {} \n Partition : {} \n Offset : {} \n Timestamp : {} ", 
                    recordMetada.topic(),
                    recordMetada.partition(),
                    recordMetada.offset(),
                    recordMetada.timestamp());
    
                }else {
                    log.error("Exception while producing :", exception);
                }
               
            });
        }
      
        kafkaProducer.close();



        
    }
}
