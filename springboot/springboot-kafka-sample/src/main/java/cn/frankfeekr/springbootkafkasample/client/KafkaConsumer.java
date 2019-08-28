package cn.frankfeekr.springbootkafkasample.client;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class);

    @KafkaListener(topics = "${app.topic.foo}")
    public void listen(ConsumerRecord<String, String> record, Acknowledgment ack, Consumer<?, ?> consumer) {
        LOG.warn("topic:{},key: {},partition:{}, value: {}, record: {}",record.topic(), record.key(),record.partition(), record.value(), record);
        if (record.topic().equalsIgnoreCase("test")){
            throw new RuntimeException();
        }
        System.out.println("提交 offset ");
        consumer.commitAsync();
    }

}

