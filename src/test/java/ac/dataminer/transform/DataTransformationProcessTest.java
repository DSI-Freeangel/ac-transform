package ac.dataminer.transform;

import ac.dataminer.transform.exception.RowFilteredOutException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DataTransformationProcessTest {

    private static final int TEST_BATCH_SIZE = 2;
    private static final HashMap<String, String> STRING_HASH_MAP = new HashMap<>();

    @Mock
    private ResultsConsumer resultsConsumer;

    @Mock
    private RowTransformation rowTransformation;

    @Mock
    private RowsDataProvider dataProvider;

    @Before
    public void setUp() {
        when(dataProvider.next(TEST_BATCH_SIZE)).thenReturn(testListOfRows(), testListOfRows(), new ArrayList<>());
        when(rowTransformation.apply(STRING_HASH_MAP)).thenReturn(STRING_HASH_MAP);
    }

    private List<Map<String, String>> testListOfRows() {
        ArrayList<Map<String, String>> list = new ArrayList<>();
        list.add(STRING_HASH_MAP);
        list.add(STRING_HASH_MAP);
        return list;
    }

    @Test
    public void allLinesProcessedSuccessfully() throws Exception {
        DataTransformationProcess process = DataTransformationProcess.builder()
                .setBatchSize(TEST_BATCH_SIZE)
                .setResultsConsumer(resultsConsumer)
                .setTransformation(rowTransformation)
                .setDataProvider(dataProvider)
                .build();
        process.run();
        verify(dataProvider, times(3)).next(TEST_BATCH_SIZE);
        verify(dataProvider, times(1)).close();
        verify(rowTransformation, times(4)).apply(any());
        verify(resultsConsumer, times(4)).accept(any());
        verify(resultsConsumer, times(1)).complete();
    }

    @Test
    public void allLinesFilteredOut() throws Exception {
        when(rowTransformation.apply(STRING_HASH_MAP)).thenThrow(new RowFilteredOutException());
        DataTransformationProcess process = DataTransformationProcess.builder()
                .setBatchSize(TEST_BATCH_SIZE)
                .setResultsConsumer(resultsConsumer)
                .setTransformation(rowTransformation)
                .setDataProvider(dataProvider)
                .build();
        process.run();
        verify(dataProvider, times(3)).next(TEST_BATCH_SIZE);
        verify(dataProvider, times(1)).close();
        verify(rowTransformation, times(4)).apply(any());
        verify(resultsConsumer, times(0)).accept(any());
        verify(resultsConsumer, times(1)).complete();
    }

    @Test
    public void allLinesProcessedSuccessfullyAsync() throws Exception {
        DataTransformationProcess process = DataTransformationProcess.builder()
                .setBatchSize(TEST_BATCH_SIZE)
                .setResultsConsumer(resultsConsumer)
                .setTransformation(rowTransformation)
                .setDataProvider(dataProvider)
                .build();
        process.start();
        while(process.isActive()) {
            Thread.sleep(36);
        }
        verify(dataProvider, times(3)).next(TEST_BATCH_SIZE);
        verify(dataProvider, times(1)).close();
        verify(rowTransformation, times(4)).apply(any());
        verify(resultsConsumer, times(4)).accept(any());
        verify(resultsConsumer, times(1)).complete();
    }

    @Test
    public void processCanBeStoppedSuccessfully(){
        DataTransformationProcess process = DataTransformationProcess.builder()
                .setBatchSize(TEST_BATCH_SIZE)
                .setResultsConsumer(resultsConsumer)
                .setTransformation(rowTransformation)
                .setDataProvider(dataProvider)
                .build();
        process.start();
        process.stop();
        assertFalse(process.isActive());
    }
}