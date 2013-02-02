package org.sakaiproject.search.indexing.exception;

import org.sakaiproject.search.indexing.Task;

/**
 * Recoverable exception occurring during the execution of a task.
 * <p>
 * This exception is recoverable and provides the recovering task to run later.
 * </p>
 *
 * @author Colin Hebert
 */
public class TemporaryTaskHandlingException extends TaskHandlingException {
    private final Task newTask;

    public TemporaryTaskHandlingException(Task newTask) {
        this.newTask = newTask;
    }

    public TemporaryTaskHandlingException(String message, Task newTask) {
        super(message);
        this.newTask = newTask;
    }

    public TemporaryTaskHandlingException(String message, Throwable cause, Task newTask) {
        super(message, cause);
        this.newTask = newTask;
    }

    public TemporaryTaskHandlingException(Throwable cause, Task newTask) {
        super(cause);
        this.newTask = newTask;
    }

    public Task getNewTask() {
        return newTask;
    }
}
