package uk.ac.ox.oucs.search.solr.indexing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ox.oucs.search.indexing.Task;
import uk.ac.ox.oucs.search.indexing.TaskHandler;
import uk.ac.ox.oucs.search.indexing.exception.TaskHandlingException;
import uk.ac.ox.oucs.search.queueing.DefaultTask;
import uk.ac.ox.oucs.search.queueing.IndexQueueing;

import java.util.Date;
import java.util.Queue;

import static uk.ac.ox.oucs.search.queueing.DefaultTask.Type.*;
import static uk.ac.ox.oucs.search.solr.indexing.SolrTask.Type.REMOVE_ALL_DOCUMENTS;

/**
 * Intercept tasks that could be split in subtasks and add them to the queuing system
 *
 * @author Colin Hebert
 */
public class SolrSplitterProcesses implements TaskHandler {
    private static final Logger logger = LoggerFactory.getLogger(SolrSplitterProcesses.class);
    private TaskHandler actualTaskHandler;
    private IndexQueueing indexQueueing;
    private SolrTools solrTools;

    @Override
    public void executeTask(Task task) {
        try {
            if (logger.isDebugEnabled())
                logger.debug("Attempt to handle '" + task + "'");
            String taskType = task.getType();
            if (INDEX_ALL.getTypeName().equals(taskType)) {
                indexAll(INDEX_SITE.getTypeName(), task.getCreationDate());
            } else if (REFRESH_ALL.getTypeName().equals(taskType)) {
                indexAll(REFRESH_SITE.getTypeName(), task.getCreationDate());
            } else {
                actualTaskHandler.executeTask(task);
            }
        } catch (TaskHandlingException e) {
            throw e;
        } catch (Exception e) {
            throw new TaskHandlingException("Couldn't execute the task '" + task + "'", e);
        }
    }

    private void indexAll(String taskType, Date creationDate) {
        final Queue<String> sites = solrTools.getIndexableSites();
        while (sites.peek() != null) {
            Task refreshSite = new SplitTask(taskType, creationDate).setProperty(DefaultTask.SITE_ID, sites.poll());
            indexQueueing.addTaskToQueue(refreshSite);
        }

        Task removeAll = new SolrTask(REMOVE_ALL_DOCUMENTS, creationDate);
        indexQueueing.addTaskToQueue(removeAll);
    }

    public void setActualTaskHandler(TaskHandler actualTaskHandler) {
        this.actualTaskHandler = actualTaskHandler;
    }

    public void setIndexQueueing(IndexQueueing indexQueueing) {
        this.indexQueueing = indexQueueing;
    }

    public void setSolrTools(SolrTools solrTools) {
        this.solrTools = solrTools;
    }

    private static class SplitTask extends DefaultTask {
        private SplitTask(String type, Date creationDate) {
            super(type, creationDate);
        }
    }
}
