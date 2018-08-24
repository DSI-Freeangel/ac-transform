package ac.dataminer.transform.exception;

/**
 * Thrown by URLMappingReader in case file structure is not valid.
 */
public class MappingFileIsNotValidException extends Exception {
    public MappingFileIsNotValidException() {
        super("Mapping file is not valid");
    }
}
