package ac.dataminer.transform.exception;

public class RowFilteredOutException extends RuntimeException {
    public RowFilteredOutException() {
        super("Row filtered out");
    }
}
