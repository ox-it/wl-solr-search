package org.sakaiproject.search.indexing.exception;

/**
 * Exception occurring during the execution of a task.
 * <p>
 * This exception is supposed to not be recoverable, otherwise use {@link TemporaryTaskHandlingException}.
 * </p>
 *
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
