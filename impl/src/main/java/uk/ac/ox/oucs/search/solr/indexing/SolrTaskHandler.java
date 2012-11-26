package uk.ac.ox.oucs.search.solr.indexing;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.util.DateUtil;
import org.sakaiproject.search.api.EntityContentProducer;
import org.sakaiproject.search.api.SearchIndexBuilder;
import org.sakaiproject.search.api.SearchService;
import org.sakaiproject.site.api.Site;
import org.sakaiproject.site.api.SiteService;
import org.springframework.beans.factory.ObjectFactory;
import uk.ac.ox.oucs.search.indexing.AbstractTaskHandler;
import uk.ac.ox.oucs.search.indexing.exception.TaskHandlingException;
import uk.ac.ox.oucs.search.indexing.exception.TemporaryTaskHandlingException;
import uk.ac.ox.oucs.search.producer.ContentProducerFactory;
import uk.ac.ox.oucs.search.queueing.DefaultTask;
import uk.ac.ox.oucs.search.indexing.Task;
import uk.ac.ox.oucs.search.solr.SolrSearchIndexBuilder;
import uk.ac.ox.oucs.search.solr.indexing.process.*;

import java.io.IOException;
import java.util.Date;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @author Colin Hebert
 */
public class SolrTaskHandler extends AbstractTaskHandler {
    private ContentProducerFactory contentProducerFactory;
    private ObjectFactory solrServerFactory;
    private SiteService siteService;
    private SearchIndexBuilder searchIndexBuilder;

    @Override
    public void executeTask(Task task) {
        super.executeTask(task);
        String taskType = task.getType();

        if (SolrTask.Type.REMOVE_SITE_DOCUMENTS.equals(taskType)) {
            removeSiteDocuments(task.getProperty(DefaultTask.SITE_ID), task.getCreationDate());
        } else if (SolrTask.Type.REMOVE_ALL_DOCUMENTS.equals(taskType)) {
            removeAllDocuments(task.getCreationDate());
        }
    }

    @Override
    protected void indexDocument(String resourceName, Date actionDate) {
        try {
            SolrServer solrServer = (SolrServer) solrServerFactory.getObject();
            EntityContentProducer contentProducer = contentProducerFactory.getContentProducerForElement(resourceName);
            new IndexDocumentProcess(solrServer, contentProducer, resourceName).execute();
            solrServer.commit();
        } catch (TaskHandlingException e) {
            throw e;
        } catch (IOException e) {
            throw new TemporaryTaskHandlingException("Couldn't index the document '" + resourceName + "'", e);
        } catch (Exception e) {
            throw new TaskHandlingException("Couldn't index the document '" + resourceName + "'", e);
        }
    }

    @Override
    protected void removeDocument(String resourceName, Date actionDate) {
        try {
            SolrServer solrServer = (SolrServer) solrServerFactory.getObject();
            EntityContentProducer contentProducer = contentProducerFactory.getContentProducerForElement(resourceName);
            new RemoveDocumentProcess(solrServer, contentProducer, resourceName).execute();
            solrServer.commit();
        } catch (TaskHandlingException e) {
            throw e;
        } catch (IOException e) {
            throw new TemporaryTaskHandlingException("Couldn't unindex the document '" + resourceName + "'", e);
        } catch (Exception e) {
            throw new TaskHandlingException("Couldn't unindex the document '" + resourceName + "'", e);
        }
    }

    @Override
    protected void indexSite(String siteId, Date actionDate) {
        try {
            SolrServer solrServer = (SolrServer) solrServerFactory.getObject();
            new BuildSiteIndexProcess(solrServer, contentProducerFactory, siteId).execute();
            solrServer.commit();
        } catch (TaskHandlingException e) {
            throw e;
        } catch (IOException e) {
            throw new TemporaryTaskHandlingException("Couldn't index the site '" + siteId + "'", e);
        } catch (Exception e) {
            throw new TaskHandlingException("Couldn't index the site '" + siteId + "'", e);
        }
    }

    @Override
    protected void refreshSite(String siteId, Date actionDate) {
        try {
            SolrServer solrServer = (SolrServer) solrServerFactory.getObject();
            new RefreshSiteIndexProcess(solrServer, contentProducerFactory, siteId).execute();
            solrServer.commit();
        } catch (TaskHandlingException e) {
            throw e;
        } catch (IOException e) {
            throw new TemporaryTaskHandlingException("Couldn't unindex the site '" + siteId + "'", e);
        } catch (Exception e) {
            throw new TaskHandlingException("Couldn't unindex the site '" + siteId + "'", e);
        }
    }

    @Override
    protected void indexAll(Date actionDate) {
        try {
            SolrServer solrServer = (SolrServer) solrServerFactory.getObject();
            new RebuildIndexProcess(solrServer, getIndexableSites(), contentProducerFactory).execute();
            solrServer.commit();
        } catch (TaskHandlingException e) {
            throw e;
        } catch (IOException e) {
            throw new TemporaryTaskHandlingException("Couldn't index the entire instance", e);
        } catch (Exception e) {
            throw new TaskHandlingException("Couldn't index the entire instance", e);
        }
    }

    @Override
    protected void refreshAll(Date actionDate) {
        try {
            SolrServer solrServer = (SolrServer) solrServerFactory.getObject();
            new RefreshIndexProcess(solrServer, getIndexableSites(), contentProducerFactory).execute();
            solrServer.commit();
        } catch (TaskHandlingException e) {
            throw e;
        } catch (IOException e) {
            throw new TemporaryTaskHandlingException("Couldn't refresh the entire instance", e);
        } catch (Exception e) {
            throw new TaskHandlingException("Couldn't refresh the entire instance", e);
        }
    }

    protected void removeSiteDocuments(String siteId, Date creationDate) {
        try {
            SolrServer solrServer = getSolrServer();
            solrServer.deleteByQuery(SearchService.DATE_STAMP + ":[* TO " + DateUtil.getThreadLocalDateFormat().format(creationDate) + "] AND " +
                    SearchService.FIELD_SITEID + ":" + ClientUtils.escapeQueryChars(siteId));
            solrServer.commit();
        } catch (IOException e) {
            throw new TemporaryTaskHandlingException("Couldn't remove old documents the site '" + siteId + "'", e);
        } catch (Exception e) {
            throw new TaskHandlingException("Couldn't remove old documents the site '" + siteId + "'", e);
        }
    }

    protected void removeAllDocuments(Date creationDate) {
        try {
            SolrServer solrServer = getSolrServer();
            solrServer.deleteByQuery(SearchService.DATE_STAMP + ":[* TO " + DateUtil.getThreadLocalDateFormat().format(creationDate) + "]");
            solrServer.commit();
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

    private SolrServer getSolrServer() {
        return (SolrServer) solrServerFactory.getObject();
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

    private boolean isSiteIndexable(Site site) {
        return !(siteService.isSpecialSite(site.getId()) ||
                (searchIndexBuilder.isOnlyIndexSearchToolSites() && site.getToolForCommonId(SolrSearchIndexBuilder.SEARCH_TOOL_ID) == null) ||
                (searchIndexBuilder.isExcludeUserSites() && siteService.isUserSite(site.getId())));
    }
}
