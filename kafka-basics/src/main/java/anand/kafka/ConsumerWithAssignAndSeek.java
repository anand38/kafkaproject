package anand.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerWithAssignAndSeek {
    static private int numberOfMessagesToBeRead=5;
    static private int numberOfMessagesRead=0;
    static private boolean areAllMessagesRead=true;

    public static void main(String[] args) {
        //create logger
        Logger logger = LoggerFactory.getLogger(Consumer.class.getName());
        //create the properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //Assign and seek is used to reply messages
        TopicPartition partitionToReadFrom=new TopicPartition("second_topic",0);

        consumer.assign(Arrays.asList(partitionToReadFrom)); //Assign

        consumer.seek(partitionToReadFrom,0L); //Seek




        //poll for data
        while (areAllMessagesRead) {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesRead++;
                logger.info(record.value() + "\n" + record.topic() + "\n" + record.partition() + "\n" + record.timestamp());
                System.out.println("Number Of Messages Read: "+numberOfMessagesRead);
                if(numberOfMessagesRead>=numberOfMessagesToBeRead){
                    areAllMessagesRead=false;
                    break;
                }
            }
        }
    }

}