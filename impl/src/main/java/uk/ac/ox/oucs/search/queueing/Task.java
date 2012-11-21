package uk.ac.ox.oucs.search.queueing;

import uk.ac.ox.oucs.search.indexing.IndexProcesses;

import java.io.Serializable;

/**
 * @author Colin Hebert
 */
public interface Task extends Serializable {
    void execute(IndexProcesses indexProcesses);
}
