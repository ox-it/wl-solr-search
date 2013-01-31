package org.sakaiproject.search.indexing;

import org.sakaiproject.search.indexing.Task;

/**
 * @author Colin Hebert
 */
public interface TaskHandler {
    void executeTask(Task task);
}
