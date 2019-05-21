package anand.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerWithThreads {
    public static void main(String[] args) {
        new ConsumerWithThreads().run();
    }

     private void run(){
        CountDownLatch latch=new CountDownLatch(1); // 1 is the number of thread
        Runnable myConsumerRunnable=new ConsumerThread(latch);
        Thread myConsumerThread =new Thread(myConsumerRunnable);
        myConsumerThread.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Caught Shutdown hook");
            ((ConsumerThread) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                System.out.println("Application has exited");
                e.printStackTrace();
            }
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            System.out.println("Application is closing");
        }
    }

    private class ConsumerThread implements Runnable{
        CountDownLatch latch;
        KafkaConsumer<String, String> consumer;
        //create logger
        Logger logger;
        private ConsumerThread(CountDownLatch latch){
            this.latch=latch;
            logger=LoggerFactory.getLogger(Consumer.class.getName());
            //create the properties
            Properties properties=new Properties();
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"my-first-group-3");
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

            consumer=new KafkaConsumer<String, String>(properties);
            //subscribe to topics
            consumer.subscribe(Collections.singletonList("second_topic"));
        }

        @Override
        public void run() {
            try {
                //poll for data
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    records.forEach(new java.util.function.Consumer<ConsumerRecord<String, String>>() {
                        @Override
                        public void accept(ConsumerRecord<String, String> stringStringConsumerRecord) {
                            logger.info(stringStringConsumerRecord.value() + "\n" + stringStringConsumerRecord.topic() + "\n" + stringStringConsumerRecord.partition() + "\n" + stringStringConsumerRecord.timestamp());
                        }
                    });
                }
            }catch(WakeupException e){
                logger.error("Received Shutdown signal");
            }finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown(){
            consumer.wakeup(); //method to interrupt consumer.poll(), it will throw wakeup exception
        }


    }
}
