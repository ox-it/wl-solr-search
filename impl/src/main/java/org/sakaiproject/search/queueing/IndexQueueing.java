package org.sakaiproject.search.queueing;

import org.sakaiproject.search.indexing.Task;

/**
 * @author Colin Hebert
 */
public interface IndexQueueing {
    public void addTaskToQueue(Task task);
}
