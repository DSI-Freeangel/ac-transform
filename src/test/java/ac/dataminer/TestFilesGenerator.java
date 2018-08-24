package ac.dataminer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TestFilesGenerator {
    private static final String DATA_FILE_NAME = "large-data.csv";
    private static final String COLUMNS_FILE_NAME = "column-mapping.csv";
    private static final String IDS_FILE_NAME = "identifier-mapping.csv";
    private static final String ID_COLUMN_NAME = "THEIR_ID";
    private static final String OUR_ID_COLUMN_NAME = "OUR_ID";
    private static final String THEIR_ID_VALUE_PREFIX = "IDX_";
    private static final String OUR_ID_VALUE_PREFIX = "IDENT_";
    private static final String COLUMN_PREFIX = "COLUMN_";
    private static final String OUR_COLUMN_PREFIX = "COL_";
    private static final String DELIMITER = "\t";

    private static final int COLUMNS_COUNT = 100;
    private static final int ROWS_COUNT = 2000;
    private static final int SKIP_COLUMNS = 5;
    private static final int SKIP_ROWS = 2;

    public static void main(String[] args) throws IOException {
        generateInputData();
        generateColumnMappings();
        generateIdMappings();
    }

    private static void generateIdMappings() throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(IDS_FILE_NAME));
        for(int i = 0; i < ROWS_COUNT; i++) {
            if(i % SKIP_ROWS == 0) continue;
            writer.write(THEIR_ID_VALUE_PREFIX + i + DELIMITER + OUR_ID_VALUE_PREFIX + i + System.lineSeparator());
        }
        writer.close();
    }

    private static void generateColumnMappings() throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(COLUMNS_FILE_NAME));
        writer.write(ID_COLUMN_NAME + DELIMITER + OUR_ID_COLUMN_NAME + System.lineSeparator());
        for(int i = 1; i < COLUMNS_COUNT; i++) {
            if(i % SKIP_COLUMNS == 0) continue;
            writer.write(COLUMN_PREFIX + i + DELIMITER + OUR_COLUMN_PREFIX + i + System.lineSeparator());
        }
        writer.close();
    }

    private static void generateInputData() throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(DATA_FILE_NAME));
        writer.write(getHeader());
        for(int i = 0; i < ROWS_COUNT; i++) {
            writer.write(getRowData(i));
        }
        writer.close();
    }

    private static String getRowData(int rowNumber) {
        List<String> columns = new ArrayList<>();
        columns.add(THEIR_ID_VALUE_PREFIX + rowNumber);
        for(int i = 1; i < COLUMNS_COUNT; i++) {
            columns.add("VALUE_" + rowNumber + "_" + i);
        }
        return formatRow(columns);
    }

    private static String getHeader() {
        List<String> columns = new ArrayList<>();
        columns.add(ID_COLUMN_NAME);
        for(int i = 1; i < COLUMNS_COUNT; i++) {
            columns.add(COLUMN_PREFIX + i);
        }
        return formatRow(columns);
    }

    private static String formatRow(List<String> columns) {
        return String.join(DELIMITER, columns) + System.lineSeparator();
    }
}
