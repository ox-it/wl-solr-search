package uk.ac.ox.oucs.search.solr.queueing;

/**
 * @author Colin Hebert
 */
public interface IndexQueueing {
    public void addTaskToQueue(Task task);
}
