package ac.dataminer.transform;

import ac.dataminer.transform.exception.MissingRequiredParameterException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;

import static org.junit.Assert.assertNotNull;

@RunWith(MockitoJUnitRunner.class)
public class DataTransformationProcessBuilderTest {

    private static final String IDENTIFIER_COLUMN_NAME = "ID";
    private static final int BATCH_SIZE = 32;

    @Mock
    private RowsDataProvider dataProvider;

    @Mock
    private ResultsConsumer resultsConsumer;

    @Mock
    private RowTransformation transformation;

    @Mock
    private ValueMapper valueMapper;

    @Test
    public void buildSuccessfulWithCustomTransformation() {
        DataTransformationProcess process = new DataTransformationProcessBuilder()
                .setDataProvider(dataProvider)
                .setResultsConsumer(resultsConsumer)
                .setTransformation(transformation)
                .build();
        assertNotNull(process);
    }

    @Test
    public void buildSuccessfulWithValueMappers() {
        DataTransformationProcess process = new DataTransformationProcessBuilder()
                .setDataProvider(dataProvider)
                .setColumnMapper(valueMapper)
                .setIdentifierMapper(valueMapper)
                .setIdentifierColumnName(IDENTIFIER_COLUMN_NAME)
                .setResultsConsumer(resultsConsumer)
                .build();
        assertNotNull(process);
    }

    @Test
    public void buildSuccessfulWithMaps() {
        DataTransformationProcess process = new DataTransformationProcessBuilder()
                .setDataProvider(dataProvider)
                .setColumnsMapping(getNotEmptyMapping())
                .setIdentifierMapping(getNotEmptyMapping())
                .setIdentifierColumnName(IDENTIFIER_COLUMN_NAME)
                .setResultsConsumer(resultsConsumer)
                .build();
        assertNotNull(process);
    }

    @Test
    public void buildSuccessfulWithoutIdentifierMapping() {
        DataTransformationProcess process = new DataTransformationProcessBuilder()
                .setDataProvider(dataProvider)
                .setColumnsMapping(getNotEmptyMapping())
                .setResultsConsumer(resultsConsumer)
                .build();
        assertNotNull(process);
    }

    @Test
    public void buildSuccessfulWithCustomBatchSize() {
        DataTransformationProcess process = new DataTransformationProcessBuilder()
                .setDataProvider(dataProvider)
                .setResultsConsumer(resultsConsumer)
                .setTransformation(transformation)
                .setBatchSize(BATCH_SIZE)
                .build();
        assertNotNull(process);
    }

    @Test(expected = MissingRequiredParameterException.class)
    public void buildFailedWithoutDataProvider() {
        new DataTransformationProcessBuilder()
                .setResultsConsumer(resultsConsumer)
                .setTransformation(transformation)
                .build();
    }

    @Test(expected = MissingRequiredParameterException.class)
    public void buildFailedWithoutResultsConsumer() {
        new DataTransformationProcessBuilder()
                .setDataProvider(dataProvider)
                .setTransformation(transformation)
                .build();
    }

    @Test(expected = MissingRequiredParameterException.class)
    public void buildFailedWithoutRowTransformation() {
        new DataTransformationProcessBuilder()
                .setDataProvider(dataProvider)
                .setResultsConsumer(resultsConsumer)
                .build();
    }

    @Test(expected = MissingRequiredParameterException.class)
    public void buildFailedWithoutIdentifierColumnName() {
        new DataTransformationProcessBuilder()
                .setDataProvider(dataProvider)
                .setColumnsMapping(getNotEmptyMapping())
                .setIdentifierMapping(getNotEmptyMapping())
                .setResultsConsumer(resultsConsumer)
                .build();
    }

    private HashMap<String, String> getNotEmptyMapping() {
        HashMap<String, String> mapping = new HashMap<>();
        mapping.put("KEY", "VALUE");
        return mapping;
    }
}