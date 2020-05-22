package com.github.barani235.kafka.streams;

import com.google.gson.JsonParser;
import com.google.gson.internal.Streams;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import com.github.barani235.kafka.configurations.aws.EC2;

public class StreamsFilterTweets {
    static Logger logger = LoggerFactory.getLogger(StreamsFilterTweets.class.getName());
    static String bootstrapServers = EC2.bootstrapServers;

    private static JsonParser jsonParser = new JsonParser();

    private static Integer extractUserFollowersFromTweet(String tweet){
        try {
            return jsonParser.parse(tweet)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        }catch(NullPointerException e){
            return 0;
        }
    }

    public static void main(String[] args) {
        //Create  properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //Create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //input topic
        KStream<String,String> inputTopic = streamsBuilder.stream("TwitterProducer1");
        KStream<String,String> filteredStream = inputTopic.filter(
                (k, tweet) -> extractUserFollowersFromTweet(tweet) > 10000
        );
        filteredStream.to("PopularTweets");

        //Build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(),properties);

        //Start the application
        kafkaStreams.start();

    }
}
