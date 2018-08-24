package ac.dataminer.transform.exception;

/**
 * Thrown by MappedRowTransformer in case row should be skipped.
 */
public class RowFilteredOutException extends RuntimeException {
    public RowFilteredOutException() {
        super("Row filtered out");
    }
}
