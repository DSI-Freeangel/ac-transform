package ac.dataminer.transform.mapper;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class MapBasedValueMapperTest {

    private static final String ORIGINAL = "ORIGINAL";
    private static final String OURS = "OURS";
    private static final String MISSING = "MISSING";
    private static final int EXPECTED_SIZE = 1;

    @Test
    public void mapReturnsValue() {
        MapBasedValueMapper mapper = new MapBasedValueMapper(getTestMappings());
        String mapped = mapper.map(ORIGINAL);
        assertEquals(OURS, mapped);
    }

    @Test
    public void mapReturnsNull() {
        MapBasedValueMapper mapper = new MapBasedValueMapper(getTestMappings());
        String mapped = mapper.map(MISSING);
        assertNull(mapped);
    }

    @Test
    public void keySetReturnsCorrectValue() {
        MapBasedValueMapper mapper = new MapBasedValueMapper(getTestMappings());
        Set<String> keys = mapper.keySet();
        assertNotNull(keys);
        assertEquals(EXPECTED_SIZE, keys.size());
        assertTrue(keys.contains(ORIGINAL));
    }

    private Map<String, String> getTestMappings() {
        Map<String, String> mappings = new HashMap<>();
        mappings.put(ORIGINAL, OURS);
        return mappings;
    }
}