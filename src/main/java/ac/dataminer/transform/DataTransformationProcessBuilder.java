package ac.dataminer.transform;

import ac.dataminer.transform.exception.MissingRequiredParameterException;
import ac.dataminer.transform.mapper.MapBasedValueMapper;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.Map;
import java.util.Optional;

/**
 * Builds DataTransformationProcess depends on parameters passed. Use DataTransformationProcess.builder() to create new instance.
 *
 * @throws MissingRequiredParameterException in case there is not enough parameters to build DataTransformationProcess.
 */
@Setter
@Accessors(chain = true)
public class DataTransformationProcessBuilder {
    private static final int DEFAULT_BATCH_SIZE = 16;
    private RowsDataProvider dataProvider;
    private int batchSize;
    private String identifierColumnName;
    private RowTransformation transformation;
    private ResultsConsumer resultsConsumer;
    private ValueMapper columnMapper;
    private ValueMapper identifierMapper;
    private Map<String, String> columnsMapping;
    private Map<String, String> identifierMapping;

    DataTransformationProcessBuilder() {
    }

    public DataTransformationProcess build() {
        MissingRequiredParameterException.assertNotNull(resultsConsumer, "Results consumer");
        DataGenerator generator = buildDataGenerator();
        RowTransformation transformation = resolveRowTransformation();
        return new DataTransformationProcess(generator, transformation, resultsConsumer);
    }

    private RowTransformation resolveRowTransformation() {
        return Optional.ofNullable(transformation).orElseGet(this::buildDefaultTransformation);
    }

    private RowTransformation buildDefaultTransformation() {
        ValueMapper columnMapper = resolveColumnMapper();
        MissingRequiredParameterException.assertNotNull(columnMapper, "Column mapping");
        ValueMapper identifierMapper = resolveIdentifierMapper();
        if(null != identifierColumnName) {
            MissingRequiredParameterException.assertNotNull(identifierMapper, "Identifier mapping");
        } else if(null != identifierMapper) {
            MissingRequiredParameterException.assertNotNull(identifierColumnName, "Identifier column name");
        }
        transformation = MappedRowTransformer.builder()
                .columnMapper(columnMapper)
                .identifierMapper(identifierMapper)
                .identifierColumnName(identifierColumnName)
                .build();
        return transformation;
    }

    private ValueMapper resolveIdentifierMapper() {
        return Optional.ofNullable(identifierMapper).orElseGet(this::buildIdentifierMapper);
    }

    private ValueMapper buildIdentifierMapper() {
        identifierMapper = Optional.ofNullable(identifierMapping)
                .filter(c -> !c.isEmpty())
                .map(MapBasedValueMapper::new)
                .orElse(null);
        return identifierMapper;
    }

    private ValueMapper resolveColumnMapper() {
        return Optional.ofNullable(columnMapper).orElseGet(this::buildColumnMapper);
    }

    private ValueMapper buildColumnMapper() {
        columnMapper = Optional.ofNullable(columnsMapping)
                .filter(c -> !c.isEmpty())
                .map(MapBasedValueMapper::new)
                .orElse(null);
        return columnMapper;
    }

    private DataGenerator buildDataGenerator() {
        MissingRequiredParameterException.assertNotNull(dataProvider, "Data provider");
        return DataGenerator.builder()
                .dataProvider(dataProvider)
                .batchSize(batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE)
                .build();
    }
}
