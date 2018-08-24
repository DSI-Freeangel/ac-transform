package ac.dataminer.runner;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Optional;
import java.util.stream.Stream;

@Getter
@RequiredArgsConstructor
public enum ParameterType {
    INPUT_FILE_URL("-f", "URL of input data file. Please use with 'file:', 'ftp:' or 'http:' protocols"),
    OUTPUT_FILE_PATH("-o", "Path for output file. This is local path so no need to use protocols"),
    COLUMNS_MAPPING_FILE_URL("-c", "URL of columns mapping file. Please use with 'file:', 'ftp:' or 'http:' protocols"),
    ID_MAPPING_FILE_URL("-i", "(Optional) URL of identifier mapping file. Please use with 'file:', 'ftp:' or 'http:' protocols. " +
            "No ID mapping will be performed in case parameters '-i' and '-ic' is not passed"),
    IDENTIFIER_COLUMN_NAME("-ic", "(Optional) Name of identifier column in input file. Required if id mapping file specified"),
    DELIMITER("-d", "(Optional) Custom delimiter to parse all input files and to use in output file. Default delimiter is \\t"),
    BATCH_SIZE("-b", "(Optional) Custom batch size for reading input file. Default value is 16");

    private final String command;
    private final String description;

    public static Optional<ParameterType> fromCommand(String command) {
        return Stream.of(ParameterType.values())
                .filter(param -> param.command.equals(command)).findAny();
    }
}
