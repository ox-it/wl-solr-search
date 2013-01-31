package org.sakaiproject.search.indexing.exception;

import java.util.Collection;
import java.util.LinkedList;

/**
 * @author Colin Hebert
 */
public class NestedTaskHandlingException extends TaskHandlingException {
    private final Collection<TaskHandlingException> taskHandlingExceptions = new LinkedList<TaskHandlingException>();

    public NestedTaskHandlingException() {
    }

    public NestedTaskHandlingException(String message) {
        super(message);
    }

    public NestedTaskHandlingException(String message, Throwable cause) {
        super(message, cause);
    }

    public NestedTaskHandlingException(Throwable cause) {
        super(cause);
    }

    public void addTaskHandlingException(TaskHandlingException t) {
        if (t instanceof NestedTaskHandlingException)
            taskHandlingExceptions.addAll(((NestedTaskHandlingException) t).getTaskHandlingExceptions());
        else
            taskHandlingExceptions.add(t);
    }

    public Collection<TaskHandlingException> getTaskHandlingExceptions() {
        return taskHandlingExceptions;
    }

    public boolean isEmpty() {
        return taskHandlingExceptions.isEmpty();
    }
}
