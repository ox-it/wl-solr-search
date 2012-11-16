package uk.ac.ox.oucs.search.solr.process;

/**
 * Process executing some Solr operations.
 * This process could be stored in a queuing system, its content should be as limited as possible (to avoid memory leaks)
 *
 * @author Colin Hebert
 */
public interface SolrProcess {
    void execute();
}
