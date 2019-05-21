package anand;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter.data.template.TwitterData;

import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumeDataFromKafka {

    public static void main(String[] args) {
        Logger logger= LoggerFactory.getLogger(ConsumeDataFromKafka.class.getName());
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"twitter-group");

        KafkaConsumer<String,String> kafkaConsumer=new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList("Twitter_Topic")); //Can subscribe to multiple topics
        PushToElasticSearch pushToElasticSearch = new PushToElasticSearch();

        String serviceName="";
        String region="";
        String aesEndpoint="";
        String document="";
        String indexingPath="/my-index/_doc";

        //Fetch Connection configuration from config for AWS ElasticSearch
        Properties esProperties=new Properties();
        FileReader reader= null;
        try {
            reader = new FileReader("D:\\OneDrive\\Projects\\ApacheKafka\\IDEAProjects\\starter\\kafka-elasticsearch-consumer\\src\\main\\resources\\properties\\awsconfig.properties");
            esProperties.load(reader);
            serviceName=esProperties.getProperty("serviceName");
            region=esProperties.getProperty("region");
            aesEndpoint=esProperties.getProperty("aesEndpoint");

        } catch (IOException e) {
            logger.error("Unable to load properties for AWS Elasticsearch...Exiting now");
            System.exit(0);

        }
        boolean isDataFetchingPending=true;

        while (isDataFetchingPending){
            ConsumerRecords<String,String> consumerRecord=kafkaConsumer.poll(Duration.ofMillis(5000));
            for (ConsumerRecord<String, String> stringStringConsumerRecord : consumerRecord) {
                logger.info("Topic: "+stringStringConsumerRecord.topic()+"Data: "+stringStringConsumerRecord.value()+"Timestamp: "+stringStringConsumerRecord.timestamp());
                Gson gson=new Gson();
                String id=gson.fromJson(stringStringConsumerRecord.value(), TwitterData.class).getId();
                //logger.info("ID :"+id);
                int statusCode=pushToElasticSearch.pushToElasticSearch(serviceName,region,aesEndpoint,stringStringConsumerRecord.value(),indexingPath+"/"+id);
                System.out.println("StatusCode: "+statusCode);
                if (statusCode!=200){
                    logger.info("Unable to connect to Elasticsearch..Exiting now");
                    isDataFetchingPending=false;
                    break;
                }

            }

        }
    }
}
