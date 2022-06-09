import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaProducerUnitTest {

    private static final String TOPIC = "topic_country_population";

    private MyKafkaProducer myKafkaProducer;
    private MockProducer<String, Integer> mockProducer;

    private void buildMockProducer(boolean autocomplete) {
        this.mockProducer = new MockProducer<>(autocomplete, new StringSerializer(), new IntegerSerializer());
    }

    @Test
    void givenKeyValue_whenSend_verifyHistory() throws ExecutionException, InterruptedException {
        buildMockProducer(true);

        //when
        myKafkaProducer = new MyKafkaProducer(mockProducer);
        Future<RecordMetadata> recordMetadataFuture = myKafkaProducer.send("data", 25);

        //then
        Assertions.assertEquals(mockProducer.history().size(), 1);
        Assertions.assertTrue(mockProducer.history().get(0).key().equalsIgnoreCase("data"));
    }
    
    @Test
    void givenKeyAndNullValue_whenSend_throwsRuntimeException() {
        buildMockProducer(true);
        
        //when
        myKafkaProducer = new MyKafkaProducer(mockProducer);
        RuntimeException e = new RuntimeException("Value Null");
        
        try {
            myKafkaProducer.send("data", null);
        } catch(Exception ex) {
            Assertions.assertEquals(e.getMessage(), ex.getMessage());
            Assertions.assertEquals(e.getClass(), ex.getClass());
        }

        Assertions.assertEquals(mockProducer.history().size(), 0);
    }

    @Test
    void givenNullKeyAndValue_whenSend_throwsRuntimeException() {
        buildMockProducer(true);

        //when
        myKafkaProducer = new MyKafkaProducer(mockProducer);
        RuntimeException e = new RuntimeException("Key Null");

        try {
            myKafkaProducer.send(null, 25);
        } catch (Exception ex){
            Assertions.assertEquals(e.getMessage(), ex.getMessage());
            Assertions.assertEquals(e.getClass(), ex.getClass());
        }

        Assertions.assertEquals(mockProducer.history().size(), 0);
    }


    /**
     * Kafka usually waits upto 1ms (linger.ms) before sending a request
     * flush() method makes all the buffered records available to send.
     * The thread that called the flush() is blocked till flush() is complete
     * while other threads can continue to work.
     *
     * When autocomplete is set to true, automatically sends all records,
     * when false, requires a flush() operation or completeNext() or errorNext()
     * operation to unblock Future object
     */
    @Test
    void givenKeyValue_whenSend_thenSendOnlyAfterFlush() {

        buildMockProducer(false);

        //when
        myKafkaProducer = new MyKafkaProducer(mockProducer);
        Future<RecordMetadata> recordMetadataFuture = myKafkaProducer.send("data", 25);
        Assertions.assertFalse(recordMetadataFuture.isDone());

        //then
        myKafkaProducer.flush();
        Assertions.assertTrue(recordMetadataFuture.isDone());
    }

    /**
     * Events that cannot be processed, for example,
     * those that don’t have the expected format
     * or are missing required attributes,
     * are routed to the error topic.
     *
     * Events for which dependent data is not available
     * are routed to a retry topic where a retry instance
     * of your application periodically attempts to process the events
     *
     * Autocomplete set to false, Future object is blocked till
     * completeNext() or errorNext() is called
     *
     * errorNext(java.lang.RuntimeException e)
     * Complete the earliest uncompleted call with the given error.
     */
    @Test
    void givenKeyValue_whenSend_thenReturnException() {

        buildMockProducer(false);

        //when
        myKafkaProducer = new MyKafkaProducer(mockProducer);
        Future<RecordMetadata> recordMetadataFuture = myKafkaProducer.send("data", 25);
        RuntimeException e = new RuntimeException();
        //completes the earliest uncompleted call that raised Runtime Exception
        mockProducer.errorNext(e);

        //then
        try {
            //ExecutionException - if the computation threw an exception
            //InterruptedException - if the current thread was interrupted while waiting
            recordMetadataFuture.get();
        } catch (ExecutionException | InterruptedException ex) {
//            System.out.println(ex.getCause());
//            System.out.println(ex);
            Assertions.assertEquals(e, ex.getCause());
        }

        Assertions.assertTrue(recordMetadataFuture.isDone());
    }
}
