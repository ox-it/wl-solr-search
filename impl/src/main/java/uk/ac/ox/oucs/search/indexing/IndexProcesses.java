package uk.ac.ox.oucs.search.indexing;

import uk.ac.ox.oucs.search.queueing.Task;

import java.util.Date;

/**
 * @author Colin Hebert
 */
public interface IndexProcesses {
    void executeTask(Task task);
}
