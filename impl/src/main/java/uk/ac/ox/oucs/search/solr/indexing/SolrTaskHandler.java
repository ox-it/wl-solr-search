package uk.ac.ox.oucs.search.solr.indexing;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.DateUtil;
import org.sakaiproject.component.cover.ComponentManager;
import org.sakaiproject.search.api.EntityContentProducer;
import org.sakaiproject.search.api.SearchIndexBuilder;
import org.sakaiproject.search.api.SearchService;
import org.sakaiproject.site.api.Site;
import org.sakaiproject.site.api.SiteService;
import org.sakaiproject.thread_local.api.ThreadLocalManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectFactory;
import uk.ac.ox.oucs.search.indexing.Task;
import uk.ac.ox.oucs.search.indexing.TaskHandler;
import uk.ac.ox.oucs.search.indexing.exception.TaskHandlingException;
import uk.ac.ox.oucs.search.indexing.exception.TemporaryTaskHandlingException;
import uk.ac.ox.oucs.search.producer.ContentProducerFactory;
import uk.ac.ox.oucs.search.queueing.DefaultTask;
import uk.ac.ox.oucs.search.solr.SolrSearchIndexBuilder;
import uk.ac.ox.oucs.search.solr.indexing.process.IndexDocumentProcess;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @author Colin Hebert
 */
public class SolrTaskHandler implements TaskHandler {
    private static final Logger logger = LoggerFactory.getLogger(SolrTaskHandler.class);
    private ContentProducerFactory contentProducerFactory;
    private ObjectFactory solrServerFactory;
    private SiteService siteService;
    private SearchIndexBuilder searchIndexBuilder;

    @Override
    public void executeTask(Task task) {
        try {
            String taskType = task.getType();
            SolrServer solrServer = (SolrServer) solrServerFactory.getObject();

            if (DefaultTask.Type.INDEX_DOCUMENT.equals(taskType)) {
                indexDocument(task.getProperty(DefaultTask.RESOURCE_NAME), task.getCreationDate(), solrServer);
            } else if (DefaultTask.Type.REMOVE_DOCUMENT.equals(taskType)) {
                removeDocument(task.getProperty(DefaultTask.RESOURCE_NAME), task.getCreationDate(), solrServer);
            } else if (DefaultTask.Type.INDEX_SITE.equals(taskType)) {
                indexSite(task.getProperty(DefaultTask.SITE_ID), task.getCreationDate(), solrServer);
            } else if (DefaultTask.Type.REFRESH_SITE.equals(taskType)) {
                refreshSite(task.getProperty(DefaultTask.SITE_ID), task.getCreationDate(), solrServer);
            } else if (DefaultTask.Type.INDEX_ALL.equals(taskType)) {
                indexAll(task.getCreationDate(), solrServer);
            } else if (DefaultTask.Type.REFRESH_ALL.equals(taskType)) {
                refreshAll(task.getCreationDate(), solrServer);
            } else if (SolrTask.Type.REMOVE_SITE_DOCUMENTS.equals(taskType)) {
                removeSiteDocuments(task.getProperty(DefaultTask.SITE_ID), task.getCreationDate(), solrServer);
            } else if (SolrTask.Type.REMOVE_ALL_DOCUMENTS.equals(taskType)) {
                removeAllDocuments(task.getCreationDate(), solrServer);
            } else {
                throw new TaskHandlingException("Task '" + task + "' can't be handled");
            }
            solrServer.commit();
        } catch (TaskHandlingException e) {
            throw e;
        } catch (IOException e) {
            throw new TemporaryTaskHandlingException("Couldn't execute the task '" + task + "'", e);
        } catch (Exception e) {
            throw new TaskHandlingException("Couldn't execute the task '" + task + "'", e);
        }
    }

    public void indexDocument(String resourceName, Date actionDate, SolrServer solrServer) {
        EntityContentProducer contentProducer = contentProducerFactory.getContentProducerForElement(resourceName);
        new IndexDocumentProcess(solrServer, contentProducer, resourceName).execute();
    }

    public void removeDocument(String resourceName, Date actionDate, SolrServer solrServer) {
        EntityContentProducer contentProducer = contentProducerFactory.getContentProducerForElement(resourceName);
        logger.debug("Remove '" + resourceName + "' from the index");
        try {
            solrServer.deleteByQuery(
                    SearchService.DATE_STAMP + ":[* TO " + format(actionDate) + "} AND " +
                    SearchService.FIELD_REFERENCE + ":" + contentProducer.getId(resourceName));
        } catch (IOException e) {
            throw new TemporaryTaskHandlingException("An exception occurred while removing the document '" + resourceName + "'", e);
        } catch (Exception e) {
            throw new TaskHandlingException("An exception occurred while removing the document '" + resourceName + "'", e);
        }
    }

    public void indexSite(final String siteId, Date actionDate, SolrServer solrServer) {
        logger.info("Rebuilding the index for '" + siteId + "'");
        try {
            for (final EntityContentProducer entityContentProducer : contentProducerFactory.getContentProducers()) {
                Iterable<String> resourceNames = new Iterable<String>() {
                    @Override
                    public Iterator<String> iterator() {
                        return entityContentProducer.getSiteContentIterator(siteId);
                    }
                };

                for (String resourceName : resourceNames) {
                    indexDocument(resourceName, actionDate, solrServer);
                }
            }
            removeSiteDocuments(siteId, actionDate, solrServer);
        } finally {
            //Clean up the localThread after each site
            ThreadLocalManager threadLocalManager = (ThreadLocalManager) ComponentManager.get(ThreadLocalManager.class);
            threadLocalManager.clear();
        }
    }

    public void refreshSite(String siteId, Date actionDate, SolrServer solrServer) {
        logger.info("Refreshing the index for '" + siteId + "'");
        try {
            //Get the currently indexed resources for this site
            Queue<String> resourceNames = getResourceNames(siteId);
            logger.debug(resourceNames.size() + " elements will be refreshed");
            while (!resourceNames.isEmpty()) {
                indexDocument(resourceNames.poll(), actionDate, solrServer);
            }
            removeSiteDocuments(siteId, actionDate, solrServer);
        } finally {
            //Clean up the localThread after each site
            ThreadLocalManager threadLocalManager = (ThreadLocalManager) ComponentManager.get(ThreadLocalManager.class);
            threadLocalManager.clear();
        }
    }

    public void indexAll(Date actionDate, SolrServer solrServer) {
        logger.info("Rebuilding the index for every indexable site");
        Queue<String> reindexedSites = getIndexableSites();
        while (!reindexedSites.isEmpty()) {
            indexSite(reindexedSites.poll(), actionDate, solrServer);
        }
        logger.info("Remove indexed documents for unindexable or non-existing sites");
        removeAllDocuments(actionDate, solrServer);
    }

    public void refreshAll(Date actionDate, SolrServer solrServer) {
        logger.info("Refreshing the index for every indexable site");
        Queue<String> refreshedSites = getIndexableSites();
        while (!refreshedSites.isEmpty()) {
            refreshSite(refreshedSites.poll(), actionDate, solrServer);
        }
    }

    public void removeSiteDocuments(String siteId, Date creationDate, SolrServer solrServer) {
        try {
            solrServer.deleteByQuery(
                    SearchService.DATE_STAMP + ":[* TO " + format(creationDate) + "} AND " +
                    SearchService.FIELD_SITEID + ":" + ClientUtils.escapeQueryChars(siteId));
        } catch (IOException e) {
            throw new TemporaryTaskHandlingException("Couldn't remove old documents the site '" + siteId + "'", e);
        } catch (Exception e) {
            throw new TaskHandlingException("Couldn't remove old documents the site '" + siteId + "'", e);
        }
    }

    public void removeAllDocuments(Date creationDate, SolrServer solrServer) {
        try {
            solrServer.deleteByQuery(SearchService.DATE_STAMP + ":[* TO " + format(creationDate) + "}");
        } catch (IOException e) {
            throw new TemporaryTaskHandlingException("Couldn't remove old documents from the entire instance", e);
        } catch (Exception e) {
            throw new TaskHandlingException("Couldn't refresh the entire instance", e);
        }
    }

    public void setContentProducerFactory(ContentProducerFactory contentProducerFactory) {
        this.contentProducerFactory = contentProducerFactory;
    }

    public void setSearchIndexBuilder(SearchIndexBuilder searchIndexBuilder) {
        this.searchIndexBuilder = searchIndexBuilder;
    }

    public void setSolrServerFactory(ObjectFactory solrServerFactory) {
        this.solrServerFactory = solrServerFactory;
    }

    public void setSiteService(SiteService siteService) {
        this.siteService = siteService;
    }

    private String format(Date creationDate) {
        return DateUtil.getThreadLocalDateFormat().format(creationDate);
    }

    private Queue<String> getIndexableSites() {
        Queue<String> refreshedSites = new LinkedList<String>();
        for (Site s : siteService.getSites(SiteService.SelectionType.ANY, null, null, null, SiteService.SortType.NONE, null)) {
            if (isSiteIndexable(s)) {
                refreshedSites.offer(s.getId());
            }
        }
        return refreshedSites;
    }

    private Queue<String> getResourceNames(String siteId) {
        try {
            SolrServer solrServer = (SolrServer) solrServerFactory.getObject();
            logger.debug("Obtaining indexed elements for site: '" + siteId + "'");
            SolrQuery query = new SolrQuery()
                    .setQuery(SearchService.FIELD_SITEID + ":" + ClientUtils.escapeQueryChars(siteId))
                    .addField(SearchService.FIELD_REFERENCE);
            SolrDocumentList results = solrServer.query(query).getResults();
            Queue<String> resourceNames = new LinkedList<String>();
            for (SolrDocument document : results) {
                resourceNames.add((String) document.getFieldValue(SearchService.FIELD_REFERENCE));
            }
            return resourceNames;
        } catch (SolrServerException e) {
            throw new TaskHandlingException("Couldn't get indexed elements for site: '" + siteId + "'", e);
        }
    }

    private boolean isSiteIndexable(Site site) {
        return !(siteService.isSpecialSite(site.getId()) ||
                (searchIndexBuilder.isOnlyIndexSearchToolSites() && site.getToolForCommonId(SolrSearchIndexBuilder.SEARCH_TOOL_ID) == null) ||
                (searchIndexBuilder.isExcludeUserSites() && siteService.isUserSite(site.getId())));
    }
}
