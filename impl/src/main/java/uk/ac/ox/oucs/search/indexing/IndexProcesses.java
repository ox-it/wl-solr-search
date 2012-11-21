package uk.ac.ox.oucs.search.indexing;

import uk.ac.ox.oucs.search.queueing.Task;

import java.util.Date;

/**
 * @author Colin Hebert
 */
public interface IndexProcesses {
    void executeTask(Task task);
    void indexDocument(String resourceName, Date indexingDate);
    void removeDocument(String resourceName, Date indexingDate);
    void indexSite(String siteId, Date indexingDate);
    void refreshSite(String siteId, Date indexingDate);
    void indexAll(Date indexingDate);
    void refreshAll(Date indexingDate);
}
