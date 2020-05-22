package com.github.barani235.kafka.producer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.barani235.kafka.configurations.aws.EC2;
import com.github.barani235.kafka.configurations.twitter.Credentials;


public class TwitterProducer {

    public TwitterProducer() {
    }

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    List<String> terms = Lists.newArrayList(new String[]{"India"});

    public void run() {
        logger.info("Setup");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue(1000);
        Client client = this.createTwitterClient(msgQueue);
        client.connect();
        KafkaProducer<String, String> producer = this.createKafkaProducer();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            this.logger.info("Stopping Application");
            client.stop();
            this.logger.info("Twitter client stopped...");
            producer.close();
            this.logger.info("Kafka producer closed...");
            this.logger.info("Done!");
        }));

        while(!client.isDone()) {
            String msg = null;

            try {
                msg = (String)msgQueue.poll(5L, TimeUnit.SECONDS);
            } catch (InterruptedException var6) {
                var6.printStackTrace();
                client.stop();
            }

            if (msg != null) {
                producer.send(new ProducerRecord("TwitterProducer1", msg), new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            TwitterProducer.this.logger.error("Something bad happened", e);
                        } else {
                            TwitterProducer.this.logger.info(recordMetadata.partition() + "\t" + recordMetadata.offset());
                        }

                    }
                });
            }
        }

        this.logger.info("End of application");
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", EC2.bootstrapServers);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("enable.idempotence", "true");
        properties.setProperty("max.in.flight.requests.per.connection", "5");
        properties.setProperty("retries", Integer.toString(Integer.MAX_VALUE));
        properties.setProperty("acks", "all");
        properties.setProperty("linger.ms", "20");
        properties.setProperty("compression.type", "snappy");
        properties.setProperty("batch.size", Integer.toString(100));
        KafkaProducer<String, String> producer = new KafkaProducer(properties);
        return producer;
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        Hosts hosebirdHosts = new HttpHosts("https://stream.twitter.com");
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        hosebirdEndpoint.trackTerms(this.terms);
        Authentication hosebirdAuth = new OAuth1(Credentials.consumerKey, Credentials.consumerSecret, Credentials.token, Credentials.secret);
        ClientBuilder builder = (new ClientBuilder()).name("Hosebird-Client-01").hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint).processor(new StringDelimitedProcessor(msgQueue));
        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }

    public static void main(String[] args) {
        (new TwitterProducer()).run();
    }

}
