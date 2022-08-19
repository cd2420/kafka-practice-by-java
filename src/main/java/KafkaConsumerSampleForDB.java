import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerSampleForDB {
    private static final String TOPIC_NAME = "my_topic_test"; // event 보낼 topic 이름

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");  // broker service 주소
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // 병렬화 설정
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // 병렬화 설정
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, TOPIC_NAME); // topic 설정

        KafkaConsumer<String, String> consumer = new KafkaConsumer(properties); // 위 설정 토대로 kafka consumer 생성
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));   // subscribe 로 event 받을 topic들 지정.

        String message = null;
        try {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100000)); // poll 로 event를 가져옴.

            for (ConsumerRecord<String, String> record : records) {
                message = record.value();       // event 값
                System.out.println(message);
            }
        } catch(Exception e) {
            // exception
        } finally {
            consumer.close();
        }
    }
}
