package uk.ac.ox.oucs.search.solr.queueing;

import org.apache.solr.client.solrj.SolrServer;
import org.sakaiproject.search.api.EntityContentProducer;
import org.sakaiproject.search.api.SearchIndexBuilder;
import org.sakaiproject.site.api.Site;
import org.sakaiproject.site.api.SiteService;
import org.sakaiproject.tool.api.Session;
import org.sakaiproject.tool.api.SessionManager;
import uk.ac.ox.oucs.search.solr.ContentProducerFactory;
import uk.ac.ox.oucs.search.solr.process.*;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutorService;

/**
 * @author Colin Hebert
 */
public class IndexQueueingImpl implements IndexQueueing {
    public static final String SEARCH_TOOL_ID = "sakai.search";
    private ExecutorService indexingExecutor;
    private SolrServer solrServer;
    private ContentProducerFactory contentProducerFactory;
    private SessionManager sessionManager;
    private SiteService siteService;
    private SearchIndexBuilder searchIndexBuilder;

    @Override
    public void addTaskToQueue(Task task) {
        indexingExecutor.execute(new RunnableProcess(task));
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
                (searchIndexBuilder.isOnlyIndexSearchToolSites() && site.getToolForCommonId(SEARCH_TOOL_ID) == null) ||
                (searchIndexBuilder.isExcludeUserSites() && siteService.isUserSite(site.getId())));
    }

    public void setIndexingExecutor(ExecutorService indexingExecutor) {
        this.indexingExecutor = indexingExecutor;
    }

    public void setSolrServer(SolrServer solrServer) {
        this.solrServer = solrServer;
    }

    public void setContentProducerFactory(ContentProducerFactory contentProducerFactory) {
        this.contentProducerFactory = contentProducerFactory;
    }

    public void setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    public void setSiteService(SiteService siteService) {
        this.siteService = siteService;
    }

    public void setSearchIndexBuilder(SearchIndexBuilder searchIndexBuilder) {
        this.searchIndexBuilder = searchIndexBuilder;
    }

    private class RunnableProcess implements Runnable {
        private final Task task;

        private RunnableProcess(Task task) {
            this.task = task;
        }

        @Override
        public void run() {
            logAsAdmin();
            try {
                SolrProcess process;
                String resourceName = task.getResourceName();
                String siteId = task.getSiteId();
                EntityContentProducer contentProducer = null;
                if (resourceName != null)
                    contentProducer = contentProducerFactory.getContentProducerForElement(resourceName);

                switch (task.getTaskType()) {
                    case INDEX_DOCUMENT:
                        process = new IndexDocumentProcess(solrServer, contentProducer, resourceName);
                        break;
                    case UNINDEX_DOCUMENT:
                        process = new RemoveDocumentProcess(solrServer, contentProducer, resourceName);
                        break;
                    case INDEX_SITE:
                        process = new BuildSiteIndexProcess(solrServer, contentProducerFactory, siteId);
                        break;
                    case REINDEX_SITE:
                        process = new RefreshSiteIndexProcess(solrServer, contentProducerFactory, siteId);
                        break;
                    case INDEX_ALL:
                        process = new RebuildIndexProcess(solrServer, getIndexableSites(), contentProducerFactory);
                        break;
                    case REINDEX_ALL:
                        process = new RefreshIndexProcess(solrServer, getIndexableSites(), contentProducerFactory);
                        break;

                    case REINDEX_DOCUMENT:
                    case UNINDEX_ALL:
                    case UNINDEX_SITE:
                    default:
                        //TODO: This exception shouldn't be caught here
                        throw new RuntimeException();
                }
                process.execute();
            } catch (Exception e) { //TODO: Catch only relevant exceptions
                addTaskToQueue(task);
            }
        }

        private void logAsAdmin() {
            Session session = sessionManager.getCurrentSession();
            session.setUserId("admin");
            session.setUserEid("admin");
        }
    }
}
