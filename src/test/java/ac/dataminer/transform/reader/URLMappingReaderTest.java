package ac.dataminer.transform.reader;

import ac.dataminer.transform.exception.MappingFileIsNotValidException;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class URLMappingReaderTest {

    private static final String DELIMITER = "\t";
    private static final int EXPECTED_SIZE = 2;

    @Test
    public void readSuccessful() throws IOException, MappingFileIsNotValidException {
        URLMappingReader mappingReader = URLMappingReader.builder()
                .url(getResourceURL("mapping-file.csv")).delimiter(DELIMITER).build();
        Map<String, String> map = mappingReader.read();
        assertNotNull(map);
        assertEquals(EXPECTED_SIZE, map.size());
    }

    @Test(expected = MappingFileIsNotValidException.class)
    public void readFailedForWrongFile() throws IOException, MappingFileIsNotValidException {
        URLMappingReader mappingReader = URLMappingReader.builder()
                .url(getResourceURL("data.csv")).delimiter(DELIMITER).build();
        mappingReader.read();
    }

    @Test(expected = MappingFileIsNotValidException.class)
    public void readFailedForEmptyFile() throws IOException, MappingFileIsNotValidException {
        URLMappingReader mappingReader = URLMappingReader.builder()
                .url(getResourceURL("empty.csv")).delimiter(DELIMITER).build();
        mappingReader.read();
    }

    @Test(expected = IOException.class)
    public void readFailedForMissingFile() throws IOException, MappingFileIsNotValidException {
        URLMappingReader mappingReader = URLMappingReader.builder()
                .url(getResourceURL("missing.txt")).delimiter(DELIMITER).build();
        mappingReader.read();
    }

    private String getResourceURL(String name) {
        URL url = this.getClass().getResource(name);
        return null != url ? url.toString() : null;
    }
}