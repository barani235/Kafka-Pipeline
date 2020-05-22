package com.gihub.barani235.kafka.consumer;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.security.user.User;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import com.github.barani235.kafka.configurations.aws.EC2;
import com.github.barani235.kafka.configurations.aws.ElasticSearch;

public class ElasticSearchConsumer {

    public static RestHighLevelClient createClient(){
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(ElasticSearch.username, ElasticSearch.password));
        RestClientBuilder builder = RestClient.builder(new HttpHost(ElasticSearch.hostname,443,"https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static KafkaConsumer<String,String> createConsumer(String topics){

        String bootstrapServers = EC2.bootstrapServers;
        //String topics = "TwitterProducer";
        String group = "elasticsearch";
        String offsetReset = "earliest";
        Properties properties = new Properties();

        //Set Consumer Properties
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,offsetReset);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"100");

        //Create KafkaConsumer object
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topics));
        return consumer;

    }

    private static JsonParser jsonParser = new JsonParser();

    private static String extractIdFromTweet(String tweet){
        return jsonParser.parse(tweet)
                        .getAsJsonObject()
                        .get("id_str")
                        .getAsString();
    }

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = createClient();
        KafkaConsumer<String,String> consumer = createConsumer("TwitterProducer1");
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            BulkRequest bulkRequest = new BulkRequest();
            Integer recordCount = records.count();
            logger.info("Received "+ recordCount +" records");
            for(ConsumerRecord<String,String> record : records){
                //Introduce idempotency
                try {
                    String id = extractIdFromTweet(record.value());
                    IndexRequest indexRequest = new IndexRequest("twitter").id(id)//.type("tweets)
                            .source(record.value(), XContentType.JSON);
                    bulkRequest.add(indexRequest);
                }catch(NullPointerException e){
                    logger.warn("Skipping bad data: " + record.value());
                }
            }
            if(recordCount > 0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest,RequestOptions.DEFAULT);
                logger.info("Committing offsets...");
                consumer.commitSync();
                logger.info("Offsets have been committed");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        //client.close();
    }
}
