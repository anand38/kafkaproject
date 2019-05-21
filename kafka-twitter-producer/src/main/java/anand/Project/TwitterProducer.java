package anand.Project;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterProducer {
     String consumerKey="";
     String consumerSecret="";
     String apiToken="";
     String apiSecret="";

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run(){
        Logger logger= LoggerFactory.getLogger(TwitterProducer.class.getName());
        BlockingQueue<String> msgQueue=new LinkedBlockingQueue<String>(100000);

        //Fetch Twitter config details

        //Fetch Connection configuration from config for AWS ElasticSearch
        Properties twitterProperties=new Properties();
        FileReader reader= null;
        try {
            reader = new FileReader("D:\\OneDrive\\Projects\\ApacheKafka\\IDEAProjects\\starter\\kafka-twitter-producer\\src\\main\\resources\\twitterconfig.properties");
            twitterProperties.load(reader);
            consumerKey=twitterProperties.getProperty("consumerKey");
            consumerSecret=twitterProperties.getProperty("consumerSecret");
            apiToken=twitterProperties.getProperty("apiToken");
            apiSecret=twitterProperties.getProperty("apiSecret");

        } catch (IOException e) {
            logger.error("Unable to load properties for AWS Elasticsearch...Exiting now");
            System.exit(0);

        }
        Client client=getTwitterClient(msgQueue);
        // Attempts to establish a connection.
        client.connect();

        //create Kafka Producer
        KafkaProducer<String, String> kafkaProducer=getKafkaProducer();

        //Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown signal received");
            client.stop();
            kafkaProducer.close();

            System.out.println("Application Stopped");
        }));
        /* Logic to send tweets data to Kafka topic */
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {

            String msg = null;
            try {
                msg = msgQueue.take();
                kafkaProducer.send(new ProducerRecord<>("Twitter_Topic", msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e!=null){
                            logger.error("Some Issues with Kafka",e);
                            kafkaProducer.close();
                        }
                    }
                });

            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
        }

    }

    public Client getTwitterClient(BlockingQueue<String> msgQueue){
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hoseBirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hoseBirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("twitter");
        hoseBirdEndpoint.followings(followings);
        hoseBirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hoseBirdAuth = new OAuth1(consumerKey, consumerSecret, apiToken, apiSecret);
        //creating a client
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hoseBirdHosts)
                .authentication(hoseBirdAuth)
                .endpoint(hoseBirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue))
                .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client client = builder.build();

        return client;
    }

    public KafkaProducer<String, String> getKafkaProducer(){
        /*  Now creating a Kafka Producer  */
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");

        //create a safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));

        KafkaProducer<String, String> kafkaProducer=new KafkaProducer<String, String>(properties);
        return kafkaProducer;
    }

}
