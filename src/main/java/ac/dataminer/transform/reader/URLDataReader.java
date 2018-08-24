package ac.dataminer.transform.reader;

import ac.dataminer.transform.RowsDataProvider;
import ac.dataminer.transform.exception.HeaderNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public class URLDataReader implements RowsDataProvider {
    private final String url;
    private final String delimiter;
    private BufferedReader reader;
    private String[] header;

    @Override
    public List<Map<String, String>> next(int linesCount) {
        List<Map<String, String>> lines = new ArrayList<>(linesCount);
        try {
            String[] header = getHeader();
            BufferedReader reader = getReader();
            if(null != reader && reader.ready()) {
                String inputLine;
                while (lines.size() < linesCount && (inputLine = reader.readLine()) != null) {
                    lines.add(buildRow(header, inputLine));
                }
            }
        } catch (Exception e) {
            log.error("Data receiving failed for url: {}", url, e);
        }
        return lines;
    }

    private Map<String, String> buildRow(String[] header, String inputLine) {
        String[] rowElements = parseLine(inputLine);
        Map<String, String> row = new HashMap<>();
        for(int index = 0; index < header.length; index++) {
            String value = rowElements.length > index ? rowElements[index] : null;
            row.put(header[index], value);
        }
        return row;
    }

    private String[] getHeader() throws IOException {
        return null != header ? header : readHeader();
    }

    private String[] readHeader() throws IOException {
        BufferedReader reader = getReader();
        String inputLine;
        if((inputLine = reader.readLine()) != null) {
            header = parseLine(inputLine);
        } else {
            throw new HeaderNotFoundException();
        }
        return header;
    }

    private String[] parseLine(String inputLine) {
        return inputLine.split(delimiter);
    }

    @Override
    public void close() {
        try {
            if(null != reader) {
                reader.close();
            }
        } catch (IOException e) {
            log.error("Error while closing data reader", e);
        }
    }

    private BufferedReader getReader() throws IOException {
        return null != reader ? reader : createReader();
    }

    private BufferedReader createReader() throws IOException {
        URL url = new URL(this.url);
        reader = new BufferedReader(
                new InputStreamReader(url.openStream()));
        return reader;
    }
}
