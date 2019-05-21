package anand.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        //create logger
        Logger logger=LoggerFactory.getLogger(Consumer.class.getName());
        //create the properties
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"my-first-group-1");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KafkaConsumer<String, String> consumer=new KafkaConsumer<String, String>(properties);

        //subscribe to topics
        consumer.subscribe(Collections.singletonList("second_topic"));

        //poll for data
        while (true){
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(100));
            records.forEach(new java.util.function.Consumer<ConsumerRecord<String, String>>() {
                @Override
                public void accept(ConsumerRecord<String, String> stringStringConsumerRecord) {
                    logger.info(stringStringConsumerRecord.value()+"\n"+stringStringConsumerRecord.topic()+"\n"+stringStringConsumerRecord.partition()+"\n"+stringStringConsumerRecord.timestamp());
                }
            });
        }
    }
}
