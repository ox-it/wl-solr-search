package uk.ac.ox.oucs.search.solr.indexing.exception;

/**
 * @author Colin Hebert
 */
public class TemporaryProcessExecutionException extends ProcessExecutionException {
    public TemporaryProcessExecutionException() {
    }

    public TemporaryProcessExecutionException(String message) {
        super(message);
    }

    public TemporaryProcessExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

    public TemporaryProcessExecutionException(Throwable cause) {
        super(cause);
    }
}
