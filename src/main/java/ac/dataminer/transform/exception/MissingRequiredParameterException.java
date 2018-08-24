package ac.dataminer.transform.exception;

import java.util.Optional;

/**
 * Thrown by DataTransformationProcessBuilder in case some required parameters are missing.
 */
public class MissingRequiredParameterException extends RuntimeException {
    private MissingRequiredParameterException(String paramName) {
        super(String.format("Required parameter '%s' is missing", paramName));
    }

    public static void assertNotNull(Object param, String name) {
        Optional.ofNullable(param).orElseThrow(() -> new MissingRequiredParameterException(name));
    }
}
