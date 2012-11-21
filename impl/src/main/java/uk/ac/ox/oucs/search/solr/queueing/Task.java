package uk.ac.ox.oucs.search.solr.queueing;

import uk.ac.ox.oucs.search.solr.indexing.IndexProcesses;

import java.io.Serializable;

/**
 * @author Colin Hebert
 */
public interface Task extends Serializable {
    void execute(IndexProcesses indexProcesses);
}
