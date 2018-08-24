package ac.dataminer.transform.consumer;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

public class ToFileResultsConsumerTest {
    private static final String TEST_FILE_NAME = "target/new-fIle.txt";
    private static final String DELIMITER = "\t";
    private ToFileResultsConsumer consumer;

    @Before
    public void setUp() {
        consumer = new ToFileResultsConsumer(TEST_FILE_NAME, DELIMITER);
    }

    @Test
    public void acceptSuccessful() throws Exception {
        consumer.accept(getTestDataMap());
        consumer.complete();
    }

    private HashMap<String, String> getTestDataMap() {
        HashMap<String, String> map = new HashMap<>();
        map.put("ID", "100");
        map.put("NAME", "HUNDRED");
        return map;
    }

}