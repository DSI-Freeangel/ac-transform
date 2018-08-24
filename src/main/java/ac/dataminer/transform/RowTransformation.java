package ac.dataminer.transform;

import java.util.Map;
import java.util.function.Function;

@FunctionalInterface
public interface RowTransformation extends Function<Map<String, String>, Map<String, String>> {
}
