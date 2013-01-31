package org.sakaiproject.search.queueing;

import org.sakaiproject.search.indexing.Task;

/**
 * @author Colin Hebert
 */
public interface TaskRunner {
    void runTask(Task task);
}
