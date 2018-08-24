package ac.dataminer.transform;

import ac.dataminer.transform.consumer.ToFileResultsConsumer;
import ac.dataminer.transform.exception.MappingFileIsNotValidException;
import ac.dataminer.transform.reader.URLDataReader;
import ac.dataminer.transform.reader.URLMappingReader;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.Map;

public class DataTransformationIntegrationTest {

    private static final String DELIMITER = "\t";
    private static final String IDENTIFIER_COLUMN_NAME = "THEIR_ID";

    private DataTransformationProcess process;

    @Before
    public void setUp() throws IOException, MappingFileIsNotValidException {
        RowsDataProvider dataProvider = new URLDataReader(getResourceURL("large-data.csv"), DELIMITER);
        Map<String, String> columnsMapping = readMappings("column-mapping.csv");
        Map<String, String> identifierMapping = readMappings("identifier-mapping.csv");
        ResultsConsumer resultsConsumer = new ToFileResultsConsumer("target/large-result.csv", DELIMITER);
        process = DataTransformationProcess.builder()
                .setDataProvider(dataProvider)
                .setColumnsMapping(columnsMapping)
                .setIdentifierMapping(identifierMapping)
                .setIdentifierColumnName(IDENTIFIER_COLUMN_NAME)
                .setResultsConsumer(resultsConsumer)
                .build();
    }

    @Test
    public void processRunOnFiles() {
        long millis = System.currentTimeMillis();
        process.run();
        long time = System.currentTimeMillis() - millis;
        System.out.println("Processing time: " + time);
    }

    private Map<String, String> readMappings(String fileName) throws IOException, MappingFileIsNotValidException {
        return URLMappingReader.builder()
                .url(getResourceURL(fileName))
                .delimiter(DELIMITER)
                .build()
                .read();
    }

    private String getResourceURL(String name) {
        URL url = this.getClass().getResource(name);
        return null != url ? url.toString() : null;
    }

}
