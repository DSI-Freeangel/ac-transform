package ac.dataminer.transform;

import java.util.Set;

public interface ValueMapper {
    String map(String original);

    Set<String> keySet();
}
