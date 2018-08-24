package ac.dataminer.transform.reader;

import org.junit.Test;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class URLDataReaderTest {
    private static final int LINES_COUNT = 10;
    private static final String TEST_DELIMITER = "\t";

    @Test
    public void readingSuccessful() {
        URLDataReader dataProvider = new URLDataReader(getResourceURL("data.csv"), TEST_DELIMITER);
        List<Map<String, String>> lines = new ArrayList<>();
        List<Map<String, String>> nextLines;
        while((nextLines = dataProvider.next(LINES_COUNT)).size() > 0) {
            assertTrue(nextLines.size() <= LINES_COUNT);
            lines.addAll(nextLines);
        }
        assertEquals(14, lines.size());
        dataProvider.close();
    }

    @Test
    public void readingReturnsEmptyListForMissingFile() {
        URLDataReader dataProvider = new URLDataReader(getResourceURL("notFound.txt"), TEST_DELIMITER);
        List<Map<String, String>> nextLines = dataProvider.next(LINES_COUNT);
        assertEquals(0, nextLines.size());
    }

    @Test
    public void readingReturnsEmptyListForEmptyFile() {
        URLDataReader dataProvider = new URLDataReader(getResourceURL("empty.csv"), TEST_DELIMITER);
        List<Map<String, String>> nextLines = dataProvider.next(LINES_COUNT);
        assertEquals(0, nextLines.size());
    }

    private String getResourceURL(String name) {
        URL url = this.getClass().getResource(name);
        return null != url ? url.toString() : null;
    }
}
