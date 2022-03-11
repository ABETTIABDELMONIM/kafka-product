package com.github.kafka;

import java.util.Properties;

import com.google.gson.JsonParser;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

public class KafkaStream {


    public static void main(String[] args) {
        // properties
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "twitter-stream-demo");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> kStream = streamsBuilder.stream("twitter_topic_V2");
        KStream<String, String> filteredStream = kStream.filter((k, v) -> {
            // filter tweets has more 100 followers
            return extractCountfollowrs(v) >= 100;
        });
        filteredStream.to("important_tweets");

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(),properties);

        kafkaStreams.start();

    }

    private static int extractCountfollowrs(String json) {
        try {
            int count  = JsonParser.parseString(json)
                        .getAsJsonObject()
                        .get("user")
                        .getAsJsonObject()
                        .get("followers_count")
                        .getAsInt();

        return count;
            
        } catch (Exception e) {
            return 0;
        }
        
        
    }
}


