package ac.dataminer.transform.exception;

/**
 * Thrown by URLDataReader in case first line of file is empty and column names can not be read
 */
public class HeaderNotFoundException extends RuntimeException {
    public HeaderNotFoundException() {
        super("Header line not found in input file");
    }
}
