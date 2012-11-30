package uk.ac.ox.oucs.search.solr.indexing;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.sakaiproject.component.cover.ComponentManager;
import org.sakaiproject.search.api.EntityContentProducer;
import org.sakaiproject.search.api.SearchService;
import org.sakaiproject.thread_local.api.ThreadLocalManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ox.oucs.search.indexing.Task;
import uk.ac.ox.oucs.search.indexing.TaskHandler;
import uk.ac.ox.oucs.search.indexing.exception.NestedTaskHandlingException;
import uk.ac.ox.oucs.search.indexing.exception.TaskHandlingException;
import uk.ac.ox.oucs.search.indexing.exception.TemporaryTaskHandlingException;
import uk.ac.ox.oucs.search.producer.ContentProducerFactory;
import uk.ac.ox.oucs.search.queueing.DefaultTask;

import java.io.IOException;
import java.util.Date;
import java.util.Queue;

import static uk.ac.ox.oucs.search.queueing.DefaultTask.Type.*;
import static uk.ac.ox.oucs.search.solr.indexing.SolrTask.Type.*;

/**
 * @author Colin Hebert
 */
public class SolrTaskHandler implements TaskHandler {
    private static final Logger logger = LoggerFactory.getLogger(SolrTaskHandler.class);
    private ContentProducerFactory contentProducerFactory;
    private SolrServer solrServer;
    private SolrTools solrTools;

    @Override
    public void executeTask(Task task) {
        logger.debug("Attempt to handle '" + task + "'");
        try {
            String taskType = task.getType();
            try {
                if (INDEX_DOCUMENT.getTypeName().equals(taskType)) {
                    indexDocument(task.getProperty(DefaultTask.RESOURCE_NAME), task.getCreationDate(), solrServer);
                } else if (REMOVE_DOCUMENT.getTypeName().equals(taskType)) {
                    removeDocument(task.getProperty(DefaultTask.RESOURCE_NAME), task.getCreationDate(), solrServer);
                } else if (INDEX_SITE.getTypeName().equals(taskType)) {
                    indexSite(task.getProperty(DefaultTask.SITE_ID), task.getCreationDate(), solrServer);
                } else if (REFRESH_SITE.getTypeName().equals(taskType)) {
                    refreshSite(task.getProperty(DefaultTask.SITE_ID), task.getCreationDate(), solrServer);
                } else if (INDEX_ALL.getTypeName().equals(taskType)) {
                    indexAll(task.getCreationDate(), solrServer);
                } else if (REFRESH_ALL.getTypeName().equals(taskType)) {
                    refreshAll(task.getCreationDate(), solrServer);
                } else if (REMOVE_SITE_DOCUMENTS.getTypeName().equals(taskType)) {
                    removeSiteDocuments(task.getProperty(DefaultTask.SITE_ID), task.getCreationDate(), solrServer);
                } else if (REMOVE_ALL_DOCUMENTS.getTypeName().equals(taskType)) {
                    removeAllDocuments(task.getCreationDate(), solrServer);
                } else if (OPTIMISE_INDEX.getTypeName().equals(taskType)) {
                    optimiseSolrIndex(solrServer);
                } else {
                    throw new TaskHandlingException("Task '" + task + "' can't be handled");
                }
            } finally {
                solrServer.commit();
            }
        } catch (Exception e) {
            throw wrapException(e, "Couldn't execute the task '" + task + "'", task);
        }
    }

    public void indexDocument(String resourceName, Date actionDate, SolrServer solrServer) {
        logger.debug("Add '" + resourceName + "' to the index");
        EntityContentProducer contentProducer = contentProducerFactory.getContentProducerForElement(resourceName);

        try {
            if (!solrTools.isDocumentOutdated(contentProducer.getId(resourceName), actionDate)) {
                logger.debug("Indexation not useful as the document was updated earlier");
                return;
            }
            solrServer.request(solrTools.toSolrRequest(resourceName, actionDate, contentProducer));
        } catch (Exception e) {
            Task task = new DefaultTask(INDEX_DOCUMENT, actionDate).setProperty(DefaultTask.RESOURCE_NAME, resourceName);
            throw wrapException(e, "An exception occurred while indexing the document '" + resourceName + "'", task);
        }
    }

    public void removeDocument(String resourceName, Date actionDate, SolrServer solrServer) {
        EntityContentProducer contentProducer = contentProducerFactory.getContentProducerForElement(resourceName);
        logger.debug("Remove '" + resourceName + "' from the index");
        try {
            solrServer.deleteByQuery(
                    SearchService.DATE_STAMP + ":{* TO " + solrTools.format(actionDate) + "} AND " +
                            SearchService.FIELD_ID + ":" + ClientUtils.escapeQueryChars(contentProducer.getId(resourceName)));
        } catch (Exception e) {
            Task task = new DefaultTask(REMOVE_DOCUMENT, actionDate).setProperty(DefaultTask.RESOURCE_NAME, resourceName);
            throw wrapException(e, "An exception occurred while removing the document '" + resourceName + "'", task);
        }
    }

    public void indexSite(final String siteId, Date actionDate, SolrServer solrServer) {
        logger.info("Rebuilding the index for '" + siteId + "'");
        try {
            NestedTaskHandlingException nthe = new NestedTaskHandlingException("An exception occured while indexing the site '" + siteId + "'");
            Queue<String> siteReferences = solrTools.getSiteDocumentsReferences(siteId);
            while (siteReferences.peek() != null) {
                try {
                    indexDocument(siteReferences.poll(), actionDate, solrServer);
                } catch (TaskHandlingException t) {
                    nthe.addTaskHandlingException(t);
                }
            }
            try {
                removeSiteDocuments(siteId, actionDate, solrServer);
            } catch (TaskHandlingException t) {
                nthe.addTaskHandlingException(t);
            }

            if (!nthe.isEmpty()) throw nthe;
        } finally {
            //Clean up the localThread after each site
            ThreadLocalManager threadLocalManager = (ThreadLocalManager) ComponentManager.get(ThreadLocalManager.class);
            threadLocalManager.clear();
        }
    }

    public void refreshSite(String siteId, Date actionDate, SolrServer solrServer) {
        logger.info("Refreshing the index for '" + siteId + "'");
        try {
            NestedTaskHandlingException nthe = new NestedTaskHandlingException("An exception occured while indexing the site '" + siteId + "'");
            //Get the currently indexed resources for this site
            Queue<String> resourceNames;
            try {
                resourceNames = solrTools.getResourceNames(siteId);
            } catch (Exception e) {
                Task task = new DefaultTask(REFRESH_SITE, actionDate).setProperty(DefaultTask.SITE_ID, siteId);
                throw wrapException(e, "Couldn't obtain the list of documents to refresh for '" + siteId + "'", task);
            }
            logger.debug(resourceNames.size() + " elements will be refreshed");
            while (!resourceNames.isEmpty()) {
                try {
                    indexDocument(resourceNames.poll(), actionDate, solrServer);
                } catch (TaskHandlingException t) {
                    nthe.addTaskHandlingException(t);
                }

            }
            try {
                removeSiteDocuments(siteId, actionDate, solrServer);
            } catch (TaskHandlingException t) {
                nthe.addTaskHandlingException(t);
            }

            if (!nthe.isEmpty()) throw nthe;
        } finally {
            //Clean up the localThread after each site
            ThreadLocalManager threadLocalManager = (ThreadLocalManager) ComponentManager.get(ThreadLocalManager.class);
            threadLocalManager.clear();
        }
    }

    public void indexAll(Date actionDate, SolrServer solrServer) {
        logger.info("Rebuilding the index for every indexable site");
        NestedTaskHandlingException nthe = new NestedTaskHandlingException("An exception occured while reindexing everything");
        Queue<String> reindexedSites = solrTools.getIndexableSites();
        while (!reindexedSites.isEmpty()) {
            try {
                indexSite(reindexedSites.poll(), actionDate, solrServer);
            } catch (TaskHandlingException t) {
                nthe.addTaskHandlingException(t);
            }
        }
        try {
            removeAllDocuments(actionDate, solrServer);
        } catch (TaskHandlingException t) {
            nthe.addTaskHandlingException(t);
        }

        if (nthe.isEmpty()) throw nthe;
    }

    public void refreshAll(Date actionDate, SolrServer solrServer) {
        logger.info("Refreshing the index for every indexable site");
        NestedTaskHandlingException nthe = new NestedTaskHandlingException("An exception occured while refreshing everything");
        Queue<String> refreshedSites = solrTools.getIndexableSites();
        while (!refreshedSites.isEmpty()) {
            try {
                refreshSite(refreshedSites.poll(), actionDate, solrServer);
            } catch (TaskHandlingException t) {
                nthe.addTaskHandlingException(t);
            }
        }
        try {
            removeAllDocuments(actionDate, solrServer);
        } catch (TaskHandlingException t) {
            nthe.addTaskHandlingException(t);
        }

        if (nthe.isEmpty()) throw nthe;
    }

    public void removeSiteDocuments(String siteId, Date creationDate, SolrServer solrServer) {
        logger.info("Remove old documents from '" + siteId + "'");
        try {
            solrServer.deleteByQuery(
                    SearchService.DATE_STAMP + ":{* TO " + solrTools.format(creationDate) + "} AND " +
                            SearchService.FIELD_SITEID + ":" + ClientUtils.escapeQueryChars(siteId));
        } catch (Exception e) {
            Task task = new SolrTask(REMOVE_SITE_DOCUMENTS, creationDate).setProperty(DefaultTask.SITE_ID, siteId);
            throw wrapException(e, "Couldn't remove old documents the site '" + siteId + "'", task);
        }
    }

    public void removeAllDocuments(Date creationDate, SolrServer solrServer) {
        logger.info("Remove old documents from every sites");
        try {
            solrServer.deleteByQuery(SearchService.DATE_STAMP + ":{* TO " + solrTools.format(creationDate) + "}");
        } catch (Exception e) {
            Task task = new SolrTask(REMOVE_ALL_DOCUMENTS, creationDate);
            throw wrapException(e, "Couldn't remove old documents from the entire instance", task);
        }
    }

    public void optimiseSolrIndex(SolrServer solrServer) {
        try {
            solrServer.optimize();
        } catch (Exception e) {
            Task task = new SolrTask(OPTIMISE_INDEX);
            throw wrapException(e, "Couldn't optimise the index", task);
        }
    }

    /**
     * Wraps an Exception in a TaskHandlingException that can be thrown
     *
     * @param e                caught Exception
     * @param message          message to associate with the wrapper
     * @param potentialNewTask new task that could be executed if the caught throwable is considered as a temporary failure
     * @return the wrapped exception or the original one if it was already wrapped
     */
    private TaskHandlingException wrapException(Exception e, String message, Task potentialNewTask) {
        if (e instanceof SolrServerException && ((SolrServerException) e).getRootCause() instanceof IOException) {
            return new TemporaryTaskHandlingException(message, e, potentialNewTask);
        } else if (e instanceof IOException) {
            return new TemporaryTaskHandlingException(message, e, potentialNewTask);
        } else if (e instanceof TaskHandlingException) {
            return (TaskHandlingException) e;
        } else {
            return new TaskHandlingException(message, e);
        }
    }

    public void setContentProducerFactory(ContentProducerFactory contentProducerFactory) {
        this.contentProducerFactory = contentProducerFactory;
    }

    public void setSolrServer(SolrServer solrServer) {
        this.solrServer = solrServer;
    }

    public void setSolrTools(SolrTools solrTools) {
        this.solrTools = solrTools;
    }
}
