package ac.dataminer.transform;

import io.reactivex.functions.Consumer;

import java.io.IOException;
import java.util.Map;

public interface ResultsConsumer extends Consumer<Map<String, String>> {

    void complete() throws IOException;
}
