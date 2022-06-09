import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;

import java.util.concurrent.Future;

public class MyKafkaProducer {
    private final Producer<String, Integer> producer;

    public MyKafkaProducer(Producer<String, Integer> producer) {
        this.producer = producer;
    }

    public Future<RecordMetadata> send(String key, Integer value) {

        if(key == null) {
            throw new RuntimeException("Key Null");
        }

        if(value == null) {
            throw new RuntimeException("Value Null");
        }

        ProducerRecord record = new ProducerRecord("topic_country_population", key, value);
        return producer.send(record);
    }

    public void flush() {
        producer.flush();
    }

    public void beginTransaction() {
        producer.beginTransaction();
    }

    public void initTransaction() {
        producer.initTransactions();
    }

    public void commitTransaction() {
        producer.commitTransaction();
    }
}

