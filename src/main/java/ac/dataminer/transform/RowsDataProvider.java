package ac.dataminer.transform;

import java.util.List;
import java.util.Map;

public interface RowsDataProvider {

    List<Map<String, String>> next(int linesCount);

    void close();
}
