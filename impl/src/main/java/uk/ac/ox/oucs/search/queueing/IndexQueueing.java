package uk.ac.ox.oucs.search.queueing;

import uk.ac.ox.oucs.search.indexing.Task;

/**
 * @author Colin Hebert
 */
public interface IndexQueueing {
    public void addTaskToQueue(Task task);
}
