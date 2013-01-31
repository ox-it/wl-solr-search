package org.sakaiproject.search.indexing.exception;

/**
 * @author Colin Hebert
 */
public class TaskHandlingException extends RuntimeException {
    public TaskHandlingException() {
    }

    public TaskHandlingException(String message) {
        super(message);
    }

    public TaskHandlingException(String message, Throwable cause) {
        super(message, cause);
    }

    public TaskHandlingException(Throwable cause) {
        super(cause);
    }
}
