package udemy.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class);

    public static void main(String[] args) {
        log.info("Kafka Consumer"); // slf4j simple: testImplementation -> implementation 으로 동작

        shutdownConsumer();
    }

    /**
     * graceful shutdown consumer
     * 1. create consumer configs
     * 2. create consumer
     * 3. get a reference to the current thread
     * 4. adding the shutdown hook
     * + exception
     * 5. subscribe consumer to our topic(s)
     * 6. poll for new data
     */
    public static void shutdownConsumer() {

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-third-application";
        String topic = "demo_java";

        //1. create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        //키,값 역직렬화
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //컨슈머 그룹 설정
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        //offset
        // none : 오프셋이 발견되지 않으면 실행하지 않음
        // earliest : 처음부터 읽음
        // latest : 지금부터 읽음
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        //2. create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);


        //3. get a reference to the current thread : 메인 스레드 획득
        final Thread mainThread = Thread.currentThread();


        //4. adding the shutdown hook : sw 종료할 때 마다 실행될 스레드
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup() ... ");
                consumer.wakeup(); // while문의 consumer.poll() 이 실행되고 예외를 발생시켜서 깨움

                // join the main thread to allow the execution of the code in the main thread: main thread에 합류시컴
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                //=> 컨슈머를 wakeup 하되 main thread가 끝날 때 까지 현재 thread(run())를 중단하지 말아라
            }
        });

        // consumer.wakeup() 으로 예외를 발생시키므로 exception 처리가 필요함
        try {
            //5. subscribe consumer to our topic(s)
            // consumer.subscribe(Collections.singleton(topic));
            consumer.subscribe(Arrays.asList(topic));


            //6. poll for new data
            while (true) {
//                log.info("Polling");

                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100)); // 지속시간: 최대 100ms 기다림

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
                // 무한루프 : 3,4를 통해 해결
            }

        } catch (WakeupException e) {
            log.info("Wake up exception!");
            // we ignore this as this is an expected exception when closing a consumer (예상 예외이기 때문에 무시)
        } catch (Exception e) {
            log.error("Unexpected exception");
        } finally {
            consumer.close(); // this will also commit the offsets if need be
            log.info("The consumer is now gracefully closed");
        }
    }

}
