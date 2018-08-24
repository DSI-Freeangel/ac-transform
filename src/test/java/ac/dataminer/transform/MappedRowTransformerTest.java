package ac.dataminer.transform;

import ac.dataminer.transform.exception.RowFilteredOutException;
import ac.dataminer.transform.mapper.MapBasedValueMapper;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class MappedRowTransformerTest {

    private static final String IDENTIFIER_COLUMN_NAME = "THEIR_ID";

    private ValueMapper columnMapper;

    private ValueMapper identifierMapper;

    @Before
    public void setUp() {
        columnMapper = new MapBasedValueMapper(getTestMapOf(IDENTIFIER_COLUMN_NAME, "OUR_ID", "COL3", "NAME"));
        identifierMapper = new MapBasedValueMapper(getTestMapOf("IDX1", "123"));
    }

    @Test
    public void rowTransformationSuccessful() {
        MappedRowTransformer transformer = MappedRowTransformer.builder()
                .identifierColumnName(IDENTIFIER_COLUMN_NAME)
                .columnMapper(columnMapper)
                .identifierMapper(identifierMapper)
                .build();
        Map<String, String> result = transformer
                .apply(getTestMapOf(IDENTIFIER_COLUMN_NAME, "IDX1", "COL2", "VALUE2", "COL3", "VALUE3"));
        assertNotNull(result);
        assertEquals(2, result.size());
        String[] expectedKeys = {"OUR_ID", "NAME"};
        assertArrayEquals(expectedKeys, result.keySet().toArray());
        String[] expectedValues = {"123", "VALUE3"};
        assertArrayEquals(expectedValues, result.values().toArray());
    }

    @Test
    public void rowTransformationSuccessfulWithoutIdentifierColumnName() {
        MappedRowTransformer transformer = MappedRowTransformer.builder()
                .columnMapper(columnMapper)
                .identifierMapper(identifierMapper)
                .build();
        Map<String, String> result = transformer
                .apply(getTestMapOf(IDENTIFIER_COLUMN_NAME, "IDX1", "COL2", "VALUE2", "COL3", "VALUE3"));
        assertNotNull(result);
        assertEquals(2, result.size());
        String[] expectedKeys = {"OUR_ID", "NAME"};
        assertArrayEquals(expectedKeys, result.keySet().toArray());
        String[] expectedValues = {"IDX1", "VALUE3"};
        assertArrayEquals(expectedValues, result.values().toArray());
    }

    @Test(expected = RowFilteredOutException.class)
    public void rowSkippedWhenIDNotExistsInMapping() {
        MappedRowTransformer transformer = MappedRowTransformer.builder()
                .identifierColumnName(IDENTIFIER_COLUMN_NAME)
                .columnMapper(columnMapper)
                .identifierMapper(identifierMapper)
                .build();
        transformer
                .apply(getTestMapOf(IDENTIFIER_COLUMN_NAME, "IDX2", "COL2", "VALUE5", "COL3", "VALUE7"));
    }

    private Map<String, String> getTestMapOf(String... values) {
        Map<String, String> row = new HashMap<>();
        for (int i = 1; i < values.length; i +=2) {
            row.put(values[i - 1], values[i]);
        }
        return row;
    }
}