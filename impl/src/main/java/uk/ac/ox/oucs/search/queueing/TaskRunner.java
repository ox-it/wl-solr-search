package uk.ac.ox.oucs.search.queueing;

import uk.ac.ox.oucs.search.indexing.Task;

/**
 * @author Colin Hebert
 */
public interface TaskRunner {
    void runTask(Task task);
}
