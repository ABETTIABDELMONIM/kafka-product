package com.github.kafka;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.LoggerFactory;

public class ElasticSearchConsumer {
   private static final org.slf4j.Logger logger= LoggerFactory.getLogger(ElasticSearchConsumer.class);
   
    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createClient();
        //create consumer
        KafkaConsumer consumer = createkafkaConsumer();
        while(true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            logger.info("Receivig {} record",records.count());
            for (ConsumerRecord<String,String> record : records) {
                try{
                    String id = extractIdFromJson(record.value());
                    IndexRequest request = new IndexRequest("twitter","tweets",id).source(record.value(), XContentType.JSON);
                    IndexResponse response = client.index(request, RequestOptions.DEFAULT);
                    logger.info("Response id : {}",response.getId());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                       logger.info("ERROR :", e);
                       client.close();
                       consumer.close();
                    }
                }catch(Exception e){
                    logger.info("ex :",e);
                }
              
            }
            logger.info("committing offser ...");
            consumer.commitSync();
            logger.info("offset committed!");

           
        }
        

    }
    private static String extractIdFromJson(String value) {
        com.fasterxml.jackson.databind.ObjectMapper mapper =  new com.fasterxml.jackson.databind.ObjectMapper();
        Map<String, String> map = null;
        try {
            map = mapper.readValue(value, Map.class);
        } catch (JsonMappingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return map.get("id_str");
    }
    private static KafkaConsumer createkafkaConsumer() {
        String bootStrapServer = "localhost:9092";
        String topic="twitter_topic_V2";
        String groupId = "elasticSearch-kafka-demo";
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"100");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
    
        //create consumer
        KafkaConsumer consumer = new KafkaConsumer<>(properties);
        //subscribe
         consumer.subscribe(Collections.singleton(topic));
         return consumer;
    }
    public static RestHighLevelClient createClient(){
        String hostname ="";
        String password ="";
        String userName ="";
     
        final CredentialsProvider credentialsProvider =  new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));
        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname,443,"https"))
                                                .setHttpClientConfigCallback((httpClientBuilder)->{
                                                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                                                }) ;

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }
    
}
