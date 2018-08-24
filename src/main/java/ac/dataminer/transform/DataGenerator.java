package ac.dataminer.transform;

import io.reactivex.Emitter;
import io.reactivex.functions.Consumer;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Map;

@Builder
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class DataGenerator implements Consumer<Emitter<List<Map<String, String>>>> {
    private final RowsDataProvider dataProvider;
    private final int batchSize;

    @Override
    public void accept(Emitter<List<Map<String, String>>> mapEmitter) {
        try {
            List<Map<String, String>> nextRows = dataProvider.next(batchSize);
            if (nextRows.size() > 0) {
                mapEmitter.onNext(nextRows);
            } else {
                dataProvider.close();
                mapEmitter.onComplete();
            }
        } catch (Throwable e) {
            dataProvider.close();
            mapEmitter.onError(e);
        }
    }
}
