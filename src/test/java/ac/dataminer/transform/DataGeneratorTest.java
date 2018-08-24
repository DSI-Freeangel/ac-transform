package ac.dataminer.transform;

import io.reactivex.Emitter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DataGeneratorTest {

    private static final int BATCH_SIZE = 1;

    @Mock
    private RowsDataProvider dataProvider;

    @Mock
    private Emitter<List<Map<String,String>>> emitter;

    private DataGenerator dataGenerator;

    @Before
    public void setUp() {
        dataGenerator = DataGenerator.builder()
                .dataProvider(dataProvider)
                .batchSize(BATCH_SIZE).build();
    }

    private ArrayList<Map<String, String>> getTestList() {
        ArrayList<Map<String, String>> list = new ArrayList<>();
        list.add(new HashMap<>());
        return list;
    }

    @Test
    public void returnsElementsFromDataProvider() {
        when(dataProvider.next(BATCH_SIZE)).thenReturn(getTestList());
        dataGenerator.accept(emitter);
        verify(emitter, times(0)).onComplete();
        verify(emitter, times(0)).onError(any());
        verify(emitter, times(1)).onNext(anyList());
        verify(dataProvider, times(0)).close();
    }

    @Test
    public void completesSuccessfullyIfNoMoreElements() {
        when(dataProvider.next(BATCH_SIZE)).thenReturn(new ArrayList<>());
        dataGenerator.accept(emitter);
        verify(emitter, times(1)).onComplete();
        verify(emitter, times(0)).onError(any());
        verify(emitter, times(0)).onNext(anyList());
        verify(dataProvider, times(1)).close();
    }

    @Test
    public void errorReturnedIfThrown() {
        when(dataProvider.next(BATCH_SIZE)).thenThrow(new RuntimeException());
        dataGenerator.accept(emitter);
        verify(emitter, times(0)).onComplete();
        verify(emitter, times(1)).onError(any());
        verify(emitter, times(0)).onNext(anyList());
        verify(dataProvider, times(1)).close();
    }
}