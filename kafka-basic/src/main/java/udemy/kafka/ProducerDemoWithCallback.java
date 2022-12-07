package udemy.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) {
        log.info("Kafka Producer"); // slf4j simple: testImplementation -> implementation 으로 동작

        callbackProducer();
    }

    /**
     * callback producer
     * 1. create producer properties
     * 2. create the producer
     * 3. create a producer record
     * 4. send data - asynchronous : callback
     * 5. flush data - synchronous
     * 6. flush and close the producer
     * + Sticky Partition vs Round Robin Partition
     */
    public static void callbackProducer() {

        //1. create producer properties
        Properties properties = new Properties();
        //properties.setProperty("key", "value");

        // 부트스트랩 브로커 정보 : $ kafka-console-producer.sh --bootstrap-server 127.0.0.1:9092
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); //"bootstrap.servers"
        // 키,값 직렬화 설정 (데이터 변환)
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //"key.serializer"
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //"value.serializer"


        //2. create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        // Sticky Partition : 모두 하나의 파티션에 저장됨 (Round Robin 아님)
        // 하나의 배치로 일괄처리해서 효율적이게 만든 것
        // partitioner.class = class org.apache.kafka.clients.producer.internals.DefaultPartitioner
        for (int i = 0; i < 10; i++) {

            //3. create a producer record : key = null
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>("demo_java", "producer callback" + i);


            //4. send data - asynchronous : callback
            producer.send(producerRecord, new Callback() {
                // 메시지 전송 완료시 호출
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // executes every time a record is successfully sent or an exception is thrown

                    if (exception == null) {
                        // the record was successfully sent
                        log.info("Received new metadata/ \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp());
                    } else {
                        log.error("Error while producing", exception);
                    }

                }
            });

            // force Round Robin
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }


        //5. flush data - synchronous
        producer.flush();


        //6. flush and close the producer
        producer.close();

    }

}
