package uk.ac.ox.oucs.search.solr.indexing;

/**
 * @author Colin Hebert
 */
public interface IndexProcesses {
    void indexDocument(String resourceName);
    void removeDocument(String resourceName);
    void indexSite(String siteId);
    void refreshSite(String siteId);
    void indexAll();
    void refreshAll();
}
