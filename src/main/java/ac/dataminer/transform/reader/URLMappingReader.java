package ac.dataminer.transform.reader;

import ac.dataminer.transform.exception.MappingFileIsNotValidException;
import lombok.Builder;
import lombok.RequiredArgsConstructor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

@Builder
@RequiredArgsConstructor
public class URLMappingReader {
    public static final int EXPECTED_COLUMNS_COUNT = 2;
    public static final int ORIGINAL_VALUE_INDEX = 0;
    public static final int NEW_VALUE_INDEX = 1;
    private final String url;
    private final String delimiter;

    public Map<String, String> read() throws IOException, MappingFileIsNotValidException {
        Map<String, String> resultMap = new HashMap<>();
        BufferedReader reader = null;
        try {
            URL url = new URL(this.url);
            reader = new BufferedReader(
                    new InputStreamReader(url.openStream()));
            String inputLine;
            while ((inputLine = reader.readLine()) != null) {
                String[] values = inputLine.split(delimiter);
                if(values.length == EXPECTED_COLUMNS_COUNT) {
                    resultMap.put(values[ORIGINAL_VALUE_INDEX], values[NEW_VALUE_INDEX]);
                } else {
                    throw new MappingFileIsNotValidException();
                }
            }
        } finally {
            if(null != reader) {
                reader.close();
            }
        }
        if(resultMap.isEmpty()) {
            throw new MappingFileIsNotValidException();
        }
        return resultMap;
    }
}
