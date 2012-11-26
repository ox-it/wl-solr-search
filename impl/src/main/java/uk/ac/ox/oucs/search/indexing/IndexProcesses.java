package uk.ac.ox.oucs.search.indexing;

import uk.ac.ox.oucs.search.queueing.Task;

import java.util.Date;

/**
 * @author Colin Hebert
 */
public interface IndexProcesses {
    void executeTask(Task task);

    void indexDocument(String resourceName, Date actionDate);

    void removeDocument(String resourceName, Date actionDate);

    void indexSite(String siteId, Date actionDate);

    void refreshSite(String siteId, Date actionDate);

    void indexAll(Date actionDate);

    void refreshAll(Date actionDate);
}
