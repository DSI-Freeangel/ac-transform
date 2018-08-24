package ac.dataminer.runner;

import ac.dataminer.transform.DataTransformationProcess;
import ac.dataminer.transform.DataTransformationProcessBuilder;
import ac.dataminer.transform.consumer.ToFileResultsConsumer;
import ac.dataminer.transform.reader.URLDataReader;
import ac.dataminer.transform.reader.URLMappingReader;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Optional;

@Slf4j
public class Application {

    private static final int MINIMUM_ARGS_LENGTH_EXPECTED = 6;

    private String inputDataFileURL;
    private String columnsMappingFileURL;
    private String identifierMappingFileURL;
    private String identifierColumnName;
    private String outputFileName;
    private String delimiter = "\t";
    private Integer batchSize;

    public static void main(String[] args) {
        Application application = new Application();
        if(args.length < MINIMUM_ARGS_LENGTH_EXPECTED) {
            application.showHelp();
            return;
        }
        application.configure(args);
        try {
            application.run();
        } catch (Exception e) {
            log.error(e.getMessage());
            application.showHelp();
        }
    }

    private void showHelp() {
        System.out.println("Available options: ");
        for(ParameterType type : ParameterType.values()) {
            System.out.println(type.getCommand() + " : " + type.getDescription());
        }
    }

    private void configure(String[] args) {
        for(int i = 1; i < args.length; i += 2) {
            final int index = i;
            Optional.ofNullable(args[index - 1])
                    .flatMap(ParameterType::fromCommand)
                    .ifPresent(type -> configureProperty(type, args[index]));
        }
    }

    private void configureProperty(ParameterType type, String value) {
        switch (type){
            case INPUT_FILE_URL: inputDataFileURL = value; break;
            case OUTPUT_FILE_PATH: outputFileName = value; break;
            case COLUMNS_MAPPING_FILE_URL: columnsMappingFileURL = value; break;
            case ID_MAPPING_FILE_URL: identifierMappingFileURL = value; break;
            case IDENTIFIER_COLUMN_NAME: identifierColumnName = value; break;
            case DELIMITER: delimiter = value; break;
            case BATCH_SIZE: batchSize = Integer.valueOf(value); break;
        }
    }

    private void run() {
        DataTransformationProcessBuilder builder = DataTransformationProcess.builder();
        Optional.ofNullable(outputFileName).map(this::buildResultsConsumer).ifPresent(builder::setResultsConsumer);
        Optional.ofNullable(inputDataFileURL).map(this::buildDataProvider).ifPresent(builder::setDataProvider);
        Optional.ofNullable(columnsMappingFileURL).map(this::readMappings).ifPresent(builder::setColumnsMapping);
        Optional.ofNullable(identifierMappingFileURL).map(this::readMappings).ifPresent(builder::setIdentifierMapping);
        Optional.ofNullable(identifierColumnName).ifPresent(builder::setIdentifierColumnName);
        Optional.ofNullable(batchSize).ifPresent(builder::setBatchSize);
        builder.build().run();
    }

    private ToFileResultsConsumer buildResultsConsumer(String filePath) {
        return new ToFileResultsConsumer(filePath, delimiter);
    }

    private URLDataReader buildDataProvider(String url) {
        return new URLDataReader(url, delimiter);
    }

    private Map<String, String> readMappings(String url) {
        try {
            return URLMappingReader.builder()
                    .url(url)
                    .delimiter(delimiter)
                    .build()
                    .read();
        } catch (Exception e) {
            log.error("failed to read mapping file: " + url);
        }
        return null;
    }
}
