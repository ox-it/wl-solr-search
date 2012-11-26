package uk.ac.ox.oucs.search.indexing.exception;

/**
 * @author Colin Hebert
 */
public class TemporaryTaskHandlingException extends TaskHandlingException {
    public TemporaryTaskHandlingException() {
    }

    public TemporaryTaskHandlingException(String message) {
        super(message);
    }

    public TemporaryTaskHandlingException(String message, Throwable cause) {
        super(message, cause);
    }

    public TemporaryTaskHandlingException(Throwable cause) {
        super(cause);
    }
}
