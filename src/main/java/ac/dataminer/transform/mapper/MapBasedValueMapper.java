package ac.dataminer.transform.mapper;

import ac.dataminer.transform.ValueMapper;
import lombok.RequiredArgsConstructor;

import java.util.Map;
import java.util.Set;

/**
 * Default implementation of ValueMapper.
 */
@RequiredArgsConstructor
public class MapBasedValueMapper implements ValueMapper {
    private final Map<String, String> mapping;

    @Override
    public String map(String original) {
        return mapping.get(original);
    }

    public Set<String> keySet() {
        return mapping.keySet();
    }
}
