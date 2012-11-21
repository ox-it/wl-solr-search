package uk.ac.ox.oucs.search.solr.indexing;

import org.apache.solr.client.solrj.SolrServer;
import org.sakaiproject.search.api.EntityContentProducer;
import org.sakaiproject.search.api.SearchIndexBuilder;
import org.sakaiproject.site.api.Site;
import org.sakaiproject.site.api.SiteService;
import org.springframework.beans.factory.ObjectFactory;
import uk.ac.ox.oucs.search.solr.ContentProducerFactory;
import uk.ac.ox.oucs.search.solr.SolrSearchIndexBuilder;
import uk.ac.ox.oucs.search.solr.process.*;
import uk.ac.ox.oucs.search.solr.process.exception.ProcessExecutionException;
import uk.ac.ox.oucs.search.solr.process.exception.TemporaryProcessExecutionException;

import java.io.IOException;
import java.util.Date;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @author Colin Hebert
 */
public class SolrIndexProcesses implements IndexProcesses {
    private ContentProducerFactory contentProducerFactory;
    private ObjectFactory solrServerFactory;
    private SiteService siteService;
    private SearchIndexBuilder searchIndexBuilder;

    @Override
    public void indexDocument(String resourceName, Date indexingDate) {
        try {
            SolrServer solrServer = (SolrServer) solrServerFactory.getObject();
            EntityContentProducer contentProducer = contentProducerFactory.getContentProducerForElement(resourceName);
            new IndexDocumentProcess(solrServer, contentProducer, resourceName).execute();
            solrServer.commit();
        } catch (ProcessExecutionException e) {
            throw e;
        } catch (IOException e) {
            throw new TemporaryProcessExecutionException("Couldn't index the document '" + resourceName + "'", e);
        } catch (Exception e) {
            throw new ProcessExecutionException("Couldn't index the document '" + resourceName + "'", e);
        }
    }

    @Override
    public void removeDocument(String resourceName, Date indexingDate) {
        try {
            SolrServer solrServer = (SolrServer) solrServerFactory.getObject();
            EntityContentProducer contentProducer = contentProducerFactory.getContentProducerForElement(resourceName);
            new RemoveDocumentProcess(solrServer, contentProducer, resourceName).execute();
            solrServer.commit();
        } catch (ProcessExecutionException e) {
            throw e;
        } catch (IOException e) {
            throw new TemporaryProcessExecutionException("Couldn't unindex the document '" + resourceName + "'", e);
        } catch (Exception e) {
            throw new ProcessExecutionException("Couldn't unindex the document '" + resourceName + "'", e);
        }
    }

    @Override
    public void indexSite(String siteId, Date indexingDate) {
        try {
            SolrServer solrServer = (SolrServer) solrServerFactory.getObject();
            new BuildSiteIndexProcess(solrServer, contentProducerFactory, siteId).execute();
            solrServer.commit();
        } catch (ProcessExecutionException e) {
            throw e;
        } catch (IOException e) {
            throw new TemporaryProcessExecutionException("Couldn't index the site '" + siteId + "'", e);
        } catch (Exception e) {
            throw new ProcessExecutionException("Couldn't index the site '" + siteId + "'", e);
        }
    }

    @Override
    public void refreshSite(String siteId, Date indexingDate) {
        try {
            SolrServer solrServer = (SolrServer) solrServerFactory.getObject();
            new RefreshSiteIndexProcess(solrServer, contentProducerFactory, siteId).execute();
            solrServer.commit();
        } catch (ProcessExecutionException e) {
            throw e;
        } catch (IOException e) {
            throw new TemporaryProcessExecutionException("Couldn't unindex the site '" + siteId + "'", e);
        } catch (Exception e) {
            throw new ProcessExecutionException("Couldn't unindex the site '" + siteId + "'", e);
        }
    }

    @Override
    public void indexAll(Date indexingDate) {
        try {
            SolrServer solrServer = (SolrServer) solrServerFactory.getObject();
            new RebuildIndexProcess(solrServer, getIndexableSites(), contentProducerFactory).execute();
            solrServer.commit();
        } catch (ProcessExecutionException e) {
            throw e;
        } catch (IOException e) {
            throw new TemporaryProcessExecutionException("Couldn't index the entire instance", e);
        } catch (Exception e) {
            throw new ProcessExecutionException("Couldn't index the entire instance", e);
        }
    }

    @Override
    public void refreshAll(Date indexingDate) {
        try {
            SolrServer solrServer = (SolrServer) solrServerFactory.getObject();
            new RefreshIndexProcess(solrServer, getIndexableSites(), contentProducerFactory).execute();
            solrServer.commit();
        } catch (ProcessExecutionException e) {
            throw e;
        } catch (IOException e) {
            throw new TemporaryProcessExecutionException("Couldn't refresh the entire instance", e);
        } catch (Exception e) {
            throw new ProcessExecutionException("Couldn't refresh the entire instance", e);
        }
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

    public void setContentProducerFactory(ContentProducerFactory contentProducerFactory) {
        this.contentProducerFactory = contentProducerFactory;
    }

    public void setSiteService(SiteService siteService) {
        this.siteService = siteService;
    }

    public void setSearchIndexBuilder(SearchIndexBuilder searchIndexBuilder) {
        this.searchIndexBuilder = searchIndexBuilder;
    }

    public void setSolrServerFactory(ObjectFactory solrServerFactory) {
        this.solrServerFactory = solrServerFactory;
    }
}
