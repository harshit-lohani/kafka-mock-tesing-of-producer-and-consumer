import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class CountryPopulationConsumerUnitTest {
    private static final String TOPIC = "topic";
    private static final int PARTITION = 0;

    private CountryPopulationConsumer countryPopulationConsumer;

    private List<CountryPopulation> updates;

    private MockConsumer<String, Integer> mockConsumer;

    private void buildMockConsumer() {
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        updates = new ArrayList<>();
        countryPopulationConsumer = new CountryPopulationConsumer(mockConsumer);
    }

    private ConsumerRecord<String, Integer> record(String topic, int partition, String country, int population) {
        return new ConsumerRecord<>(topic, partition, 0, country, population);
    }

    @Test
    void whenStartBySubscribingToTopic_thenExpectedUpdatesAreConsumedNormally() {
        //given
        buildMockConsumer();
        countryPopulationConsumer.startByAssigning(TOPIC, PARTITION);

        HashMap<TopicPartition, Long> startOffsets = new HashMap<>();
        TopicPartition tp = new TopicPartition(TOPIC, 0);
        startOffsets.put(tp, 0L);
        mockConsumer.updateBeginningOffsets(startOffsets);

        ConsumerRecord<String, Integer> rec1 = record(TOPIC, PARTITION, "India", 1_250_000_000);
        ConsumerRecord<String, Integer> rec2 = record(TOPIC, PARTITION, "Romania", 140_400_000);
        mockConsumer.addRecord(rec1);
        mockConsumer.addRecord(rec2);

        countryPopulationConsumer.consume(TOPIC, PARTITION);


//        Assertions.assertEquals(1, updates.size());
    }
}
