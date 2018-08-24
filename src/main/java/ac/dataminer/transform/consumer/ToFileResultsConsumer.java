package ac.dataminer.transform.consumer;

import ac.dataminer.transform.ResultsConsumer;
import lombok.RequiredArgsConstructor;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Default implementation of result consumer.
 * Writes rows into local file.
 *
 * Required arguments:
 * - path - output file path to write data;
 * - delimiter - delimiter to be used between columns in output file.
 */
@RequiredArgsConstructor
public class ToFileResultsConsumer implements ResultsConsumer {
    private final String path;
    private final String delimiter;
    private Set<String> header;
    private BufferedWriter writer;

    @Override
    public void accept(Map<String, String> stringStringMap) throws Exception {
        BufferedWriter writer = getWriter(stringStringMap::keySet);
        writer.write(formatDataRow(stringStringMap));
    }

    private String formatDataRow(Map<String, String> dataRow) {
        return header.stream().map(dataRow::get).collect(Collectors.joining(delimiter)) + System.lineSeparator();
    }

    private BufferedWriter getWriter(Supplier<Set<String>> headerSupplier) throws IOException {
        return null != writer ? writer : this.createWriter(headerSupplier);
    }

    private BufferedWriter createWriter(Supplier<Set<String>> headerSupplier) throws IOException {
        writer = new BufferedWriter(new FileWriter(path));
        header = headerSupplier.get();
        writer.write(formatHeader(header));
        return writer;
    }

    private String formatHeader(Set<String> header) {
        return String.join(delimiter, header) + System.lineSeparator();
    }

    @Override
    public void complete() throws IOException {
        if(null != writer) {
            writer.close();
        }
    }

}
