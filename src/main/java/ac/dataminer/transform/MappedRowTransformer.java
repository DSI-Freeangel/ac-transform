package ac.dataminer.transform;

import ac.dataminer.transform.exception.RowFilteredOutException;
import lombok.Builder;
import lombok.RequiredArgsConstructor;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Builder
@RequiredArgsConstructor
public class MappedRowTransformer implements RowTransformation {
    private final ValueMapper columnMapper;
    private final ValueMapper identifierMapper;
    private final String identifierColumnName;

    @Override
    public Map<String, String> apply(Map<String, String> originalRow) {
        return columnMapper.keySet().stream()
                .map((column) -> mapColumnValues(column, originalRow.get(column)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Map.Entry<String, String> mapColumnValues(String column, String originalValue) {
        String newColumnName = columnMapper.map(column);
        String newColumnValue = isIdentifierColumn(column) ?
                mapIdentifier(originalValue) : originalValue;

        return new AbstractMap.SimpleImmutableEntry<>(newColumnName, newColumnValue);
    }

    private boolean isIdentifierColumn(String column) {
        return null != identifierColumnName && identifierColumnName.equals(column);
    }

    private String mapIdentifier(String originalValue) {
        return Optional.ofNullable(originalValue).map(identifierMapper::map).orElseThrow(RowFilteredOutException::new);
    }
}
