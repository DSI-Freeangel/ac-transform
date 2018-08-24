package ac.dataminer.transform.exception;

public class HeaderNotFoundException extends RuntimeException {
    public HeaderNotFoundException() {
        super("Header line not found in input file");
    }
}
