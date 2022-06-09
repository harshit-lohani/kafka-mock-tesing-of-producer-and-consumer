import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CountryPopulationConsumer {

    private Consumer<String, Integer> consumer;
    private List<CountryPopulation> countryPopulationList;

    public CountryPopulationConsumer(Consumer<String, Integer> consumer) {
        this.consumer = consumer;
        countryPopulationList = new ArrayList<>();
    }

    void startBySubscribing(String topic) {
        consumer.subscribe(Collections.singleton(topic));
    }

    void startByAssigning(String topic, int partition) {
        consumer.assign(Collections.singleton(new TopicPartition(topic, partition)));
    }

    public void process(ConsumerRecord<String, Integer> record) {
        System.out.println("Processes record: key='" + record.key() + "' value=" + record.value() + "';");
    }

    public void consume(String topic, int partition) {
        try {
//            startByAssigning(topic, partition);
            while (true) {
                ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, Integer> record: records
                     ) {
                    process(record);
                }
                Thread.sleep(1000);
                System.out.println("Iteration");
            }
        } catch (WakeupException e) {
            System.out.println("Shutting down");
        } catch (RuntimeException e) {
            System.out.println("Error!");
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }


}
