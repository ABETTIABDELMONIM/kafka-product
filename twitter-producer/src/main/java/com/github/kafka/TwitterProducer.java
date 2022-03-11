package com.github.kafka;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class TwitterProducer {

    org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TwitterProducer.class.getName());

    

    private static final  String  CONSUMER_KEY = "j2DgJiLbQZl1Q7fratXLmTY4R";
    private static final String  CONSUMER_SECRET = "GwfJwKrn0elSBHL81kTVzHjcumgxwt6iWmOyxOByFt3NrdQiQU";
    private static final  String TOKEN = "115528591-oNLixboJfQemP70pMiGttohAPxaXGcHR0VCpjwW6";
    private static final  String SECRET = "ZdjRPSTxICVR4hwvvcRCX6d5FGVoUuAZKR4PCxh3P3kU1";
    private static final  String TOPIC = "twitter_topic_V2";

    



    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run(){
        //create twitter client
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        logger.info("Setup the app");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        Client client = createTwitterClient(msgQueue);
        // Attempts to establish a connection.
        logger.info("Connecting ....");
        client.connect();


        //create Kafka producer
        KafkaProducer<String,String> producer = createKafkaProducer();

        //add a shutting down hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Stoppig the application ....");
            logger.info("Shutting down the twitter client  ....");
            client.stop();
            logger.info("Closing the producer ....");
            producer.close();
            logger.info("done!");
        }));

        //send tweet to kafla
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg != null){
                
                    ProducerRecord<String,String> record = new ProducerRecord<String,String>(TOPIC, null,msg);
                    producer.send(record,(recordMetada, exception) ->{
                       if(exception != null){
                            logger.error("Exception while producing :", exception);
                        }
                    
                    });
               
            }
        }
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        Properties properties =  new Properties();
        //global condig
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Safe producer
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        //Hight throuput producer (at the expence of some latency and CPU)
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

        //create the producer
        KafkaProducer<String,String> kafkaProducer =  new KafkaProducer<>(properties);
        return kafkaProducer;
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue){

        

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
      
        List<String> terms = Lists.newArrayList("bitcoin","sport","usa","politics","soccer");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET);
        ClientBuilder builder = new ClientBuilder()
                                            .name("Hosebird-Client-01")                              // optional: mainly for the logs
                                            .hosts(hosebirdHosts)
                                            .authentication(hosebirdAuth)
                                            .endpoint(hosebirdEndpoint)
                                            .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
       return hosebirdClient;

    }
    
}
