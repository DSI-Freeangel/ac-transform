package ac.dataminer.transform.exception;

import java.util.Optional;

public class MissingRequiredParameterException extends RuntimeException {
    private MissingRequiredParameterException(String paramName) {
        super(String.format("Required parameter '%s' is missing", paramName));
    }

    public static void assertNotNull(Object param, String name) {
        Optional.ofNullable(param).orElseThrow(() -> new MissingRequiredParameterException(name));
    }
}
