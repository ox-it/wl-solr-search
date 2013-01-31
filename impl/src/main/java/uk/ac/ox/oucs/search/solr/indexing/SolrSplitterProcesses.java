package uk.ac.ox.oucs.search.solr.indexing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sakaiproject.search.indexing.Task;
import org.sakaiproject.search.indexing.TaskHandler;
import org.sakaiproject.search.indexing.exception.TaskHandlingException;
import org.sakaiproject.search.queueing.DefaultTask;
import org.sakaiproject.search.queueing.IndexQueueing;

import java.util.Date;
import java.util.Queue;

import static org.sakaiproject.search.queueing.DefaultTask.Type.*;
import static uk.ac.ox.oucs.search.solr.indexing.SolrTask.Type.OPTIMISE_INDEX;
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
                createTaskForEverySite(INDEX_SITE, task.getCreationDate());
            } else if (REFRESH_ALL.getTypeName().equals(taskType)) {
                createTaskForEverySite(REFRESH_SITE, task.getCreationDate());
            } else {
                actualTaskHandler.executeTask(task);
            }
        } catch (TaskHandlingException e) {
            throw e;
        } catch (Exception e) {
            throw new TaskHandlingException("Couldn't execute the task '" + task + "'", e);
        }
    }

    private void createTaskForEverySite(DefaultTask.Type taskType, Date creationDate) {
        Queue<String> sites = solrTools.getIndexableSites();
        while (sites.peek() != null) {
            Task refreshSite = new DefaultTask(taskType, creationDate).setProperty(DefaultTask.SITE_ID, sites.poll());
            indexQueueing.addTaskToQueue(refreshSite);
        }

        //Clean up the index by removing sites/documents that shouldn't be indexed anymore
        Task removeAll = new SolrTask(REMOVE_ALL_DOCUMENTS, creationDate);
        indexQueueing.addTaskToQueue(removeAll);
        //Start an optimisation when everything has been indexed.
        //Even if the optimisation isn't exactly the very last operation to run, it's good enough
        Task optimise = new SolrTask(OPTIMISE_INDEX, creationDate);
        indexQueueing.addTaskToQueue(optimise);
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
}
