package anand.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger= LoggerFactory.getLogger(Producer.class);
        //create property
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        //create producer
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);

        //create Producer Record
        for (int i = 0; i < 10; i++) {
            ProducerRecord record=new ProducerRecord("second_topic","id_"+Integer.toString(i),"Hello World "+Integer.toString(i));

            //send message
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e==null){
                        logger.info("Topic: "+recordMetadata.topic()+"Partition: "+recordMetadata.partition()+"Offset: "+recordMetadata.offset()+"Timestamp: "+recordMetadata.timestamp());
                    }else{
                        logger.error("Error occured while sending message: "+e.getStackTrace());
                    }
                }
            }).get();

        }
        //producer.flush();
        //flush and close
        producer.close();

    }
}
