package uk.ac.ox.oucs.search.solr.indexing;

import uk.ac.ox.oucs.search.indexing.Task;
import uk.ac.ox.oucs.search.indexing.TaskHandler;
import uk.ac.ox.oucs.search.indexing.exception.TaskHandlingException;
import uk.ac.ox.oucs.search.queueing.DefaultTask;
import uk.ac.ox.oucs.search.queueing.IndexQueueing;

import java.util.Date;
import java.util.Queue;

import static uk.ac.ox.oucs.search.queueing.DefaultTask.Type.*;
import static uk.ac.ox.oucs.search.solr.indexing.SolrTask.Type.REMOVE_ALL_DOCUMENTS;
import static uk.ac.ox.oucs.search.solr.indexing.SolrTask.Type.REMOVE_SITE_DOCUMENTS;

/**
 * Intercept tasks that could be split in subtasks and add them to the queuing system
 *
 * @author Colin Hebert
 */
public class SolrSplitterProcesses implements TaskHandler {
    private TaskHandler actualTaskHandler;
    private IndexQueueing indexQueueing;
    private SolrTools solrTools;

    @Override
    public void executeTask(Task task) {
        try {
            String taskType = task.getType();
            if (INDEX_SITE.equals(taskType)) {
                String siteId = task.getProperty(DefaultTask.SITE_ID);
                Queue<String> references = solrTools.getSiteDocumentsReferences(siteId);

                indexDocumentList(task.getCreationDate(), siteId, references);
            } else if (REFRESH_SITE.equals(taskType)) {
                String siteId = task.getProperty(DefaultTask.SITE_ID);
                Queue<String> references = solrTools.getResourceNames(siteId);

                indexDocumentList(task.getCreationDate(), siteId, references);
            } else if (INDEX_ALL.equals(taskType)) {
                indexAll(INDEX_SITE.getTypeName(), task.getCreationDate());
            } else if (REFRESH_ALL.equals(taskType)) {
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
            Task refreshSite = new DefaultTask(taskType, creationDate) {
                {
                    setProperty(DefaultTask.SITE_ID, sites.poll());
                }
            };
            indexQueueing.addTaskToQueue(refreshSite);
        }

        Task removeAll = new SolrTask(REMOVE_ALL_DOCUMENTS, creationDate);
        indexQueueing.addTaskToQueue(removeAll);
    }

    private void indexDocumentList(Date creationDate, String siteId, Queue<String> references) {
        while (references.peek() != null) {
            Task indexDocument = new DefaultTask(INDEX_DOCUMENT, creationDate)
                    .setProperty(DefaultTask.RESOURCE_NAME, references.poll());
            indexQueueing.addTaskToQueue(indexDocument);
        }

        Task removeSites = new SolrTask(REMOVE_SITE_DOCUMENTS, creationDate)
                .setProperty(DefaultTask.SITE_ID, siteId);
        indexQueueing.addTaskToQueue(removeSites);
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
