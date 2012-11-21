package uk.ac.ox.oucs.search.indexing;

/**
 * @author Colin Hebert
 */
public class ProcessExecutionException extends RuntimeException {
    public ProcessExecutionException() {
    }

    public ProcessExecutionException(String message) {
        super(message);
    }

    public ProcessExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProcessExecutionException(Throwable cause) {
        super(cause);
    }
}
