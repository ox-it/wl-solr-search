package uk.ac.ox.oucs.search.indexing.exception;

import uk.ac.ox.oucs.search.indexing.Task;

/**
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
