package uk.ac.ox.oucs.search.solr;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.util.NamedList;
import org.sakaiproject.event.api.Event;
import org.sakaiproject.event.api.Notification;
import org.sakaiproject.exception.IdUnusedException;
import org.sakaiproject.search.api.EntityContentProducer;
import org.sakaiproject.search.api.SearchIndexBuilder;
import org.sakaiproject.search.api.SearchService;
import org.sakaiproject.search.model.SearchBuilderItem;
import org.sakaiproject.site.api.Site;
import org.sakaiproject.site.api.SiteService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ox.oucs.search.solr.process.*;
import uk.ac.ox.oucs.search.solr.util.AdminStatRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * @author Colin Hebert
 */
public class SolrSearchIndexBuilder implements SearchIndexBuilder {
    public static final String SEARCH_TOOL_ID = "sakai.search";
    private static final Logger logger = LoggerFactory.getLogger(SolrSearchIndexBuilder.class);
    private SiteService siteService;
    private SolrServer solrServer;
    private ContentProducerFactory contentProducerFactory;
    private boolean searchToolRequired;
    private boolean ignoreUserSites;
    private Executor indexingExecutor;

    @Override
    public void addResource(Notification notification, Event event) {
        logger.debug("Attempt to add or remove a resource from the index");
        String resourceName = event.getResource();
        //Set the resource name to empty instead of null
        if (resourceName == null)
            //TODO: Shouldn't addResource just stop there instead?
            resourceName = "";

        EntityContentProducer entityContentProducer = contentProducerFactory.getContentProducerForEvent(event);
        //If there is no matching entity content producer or no associated site, return
        if (entityContentProducer == null) {
            logger.debug("Can't find an entityContentProducer for '" + resourceName + "'");
            return;
        }

        //If the indexing is only enabled on sites with search tool, check that the tool is actually enabled
        if (isOnlyIndexSearchToolSites()) {
            String siteId = entityContentProducer.getSiteId(resourceName);
            try {
                if (siteService.getSite(siteId).getToolForCommonId(SEARCH_TOOL_ID) == null) {
                    logger.debug("Can't index content if the search tool isn't activated. Site: " + siteId);
                    return;
                }
            } catch (IdUnusedException e) {
                logger.warn("Couldn't find the site '" + siteId + "'", e);
                return;
            }
        }

        IndexAction action = IndexAction.getAction(entityContentProducer.getAction(event));
        logger.debug("Action on '" + resourceName + "' detected as " + action.name());

        SolrProcess solrProcess;
        switch (action) {
            case ADD:
                solrProcess = new IndexDocumentProcess(solrServer, entityContentProducer, resourceName);
                break;
            case DELETE:
                solrProcess = new RemoveDocumentProcess(solrServer, entityContentProducer, resourceName);
                break;
            default:
                throw new UnsupportedOperationException(action + " is not yet supported");
        }
        indexingExecutor.execute(solrProcess);
    }

    @Override
    @Deprecated
    /**
     * @deprecated Use {@link ContentProducerFactory#addContentProducer(org.sakaiproject.search.api.EntityContentProducer)} instead
     */
    public void registerEntityContentProducer(EntityContentProducer ecp) {
        contentProducerFactory.addContentProducer(ecp);
    }

    @Override
    @Deprecated
    /**
     * @deprecated Use {@link ContentProducerFactory#getContentProducerForElement(String)} instead
     */
    public EntityContentProducer newEntityContentProducer(String ref) {
        return contentProducerFactory.getContentProducerForElement(ref);
    }

    @Override
    @Deprecated
    /**
     * @deprecated Use {@link ContentProducerFactory#getContentProducerForEvent(org.sakaiproject.event.api.Event)} instead
     */
    public EntityContentProducer newEntityContentProducer(Event event) {
        return contentProducerFactory.getContentProducerForEvent(event);
    }

    @Override
    @Deprecated
    /**
     * @deprecated Use {@link ContentProducerFactory#getContentProducers()} instead
     */
    public List<EntityContentProducer> getContentProducers() {
        return new ArrayList<EntityContentProducer>(contentProducerFactory.getContentProducers());
    }

    @Override
    public void refreshIndex(String currentSiteId) {
        indexingExecutor.execute(new RefreshSiteIndexProcess(solrServer, contentProducerFactory, currentSiteId));
    }


    @Override
    public void rebuildIndex(final String currentSiteId) {
        indexingExecutor.execute(new BuildSiteIndexProcess(solrServer, contentProducerFactory, currentSiteId));
    }

    @Override
    public void refreshIndex() {
        logger.info("Refreshing the index for every indexable site");
        for (Site s : siteService.getSites(SiteService.SelectionType.ANY, null, null, null, SiteService.SortType.NONE, null)) {
            if (isSiteIndexable(s)) {
                refreshIndex(s.getId());
            }
        }
    }

    @Override
    public boolean isBuildQueueEmpty() {
        return getPendingDocuments() == 0;
    }

    @Override
    public void rebuildIndex() {
        logger.info("Rebuilding the index for every indexable site");
        final Collection<String> reindexedSites = new LinkedList<String>();
        for (Site s : siteService.getSites(SiteService.SelectionType.ANY, null, null, null, SiteService.SortType.NONE, null)) {
            if (isSiteIndexable(s)) {
                reindexedSites.add(s.getId());
                rebuildIndex(s.getId());
            }
        }
        indexingExecutor.execute(new Runnable() {
            @Override
            public void run() {
                logger.info("Remove indexed documents for unindexable or non-existing sites");
                StringBuilder sb = new StringBuilder();
                for (String siteId : reindexedSites) {
                    sb.append(" -\"").append(siteId).append('"');
                }
                try {
                    solrServer.deleteByQuery(SearchService.FIELD_SITEID + ":( " + sb + " )");
                } catch (SolrServerException e) {
                    logger.warn("Couldn't remove obsoletes sites from the index", e);
                } catch (IOException e) {
                    logger.error("Can't contact the search server", e);
                }
            }
        });
    }

    /**
     * Check if a site is considered as indexable based on the current server configuration.
     * <p>
     * Not indexable sites are:
     * <ul>
     * <li>Special sites</li>
     * <li>Sites without the search tool (if the option is enabled)</li>
     * <li>User sites (if the option is enabled)</li>
     * </ul>
     * </p>
     *
     * @param site site which may be indexable
     * @return true if the site can be index, false otherwise
     */
    private boolean isSiteIndexable(Site site) {
        logger.debug("Check if '" + site.getId() + "' is indexable.");
        return !(siteService.isSpecialSite(site.getId()) ||
                (isOnlyIndexSearchToolSites() && site.getToolForCommonId("sakai.search") == null) ||
                (isExcludeUserSites() && siteService.isUserSite(site.getId())));
    }

    @Override
    public void destroy() {
        //TODO: Nope, we don't kill the search that easily
    }

    @Override
    public int getPendingDocuments() {
        try {
            AdminStatRequest adminStatRequest = new AdminStatRequest();
            adminStatRequest.setParam("key", "updateHandler");
            NamedList<Object> result = solrServer.request(adminStatRequest);
            NamedList<Object> mbeans = (NamedList<Object>) result.get("solr-mbeans");
            NamedList<Object> updateHandler = (NamedList<Object>) mbeans.get("UPDATEHANDLER");
            NamedList<Object> updateHandler2 = (NamedList<Object>) updateHandler.get("updateHandler");
            NamedList<Object> stats = (NamedList<Object>) updateHandler2.get("stats");
            return ((Long) stats.get("docsPending")).intValue();
        } catch (SolrServerException e) {
            logger.warn("Couldn't obtain the number of pending documents", e);
            return 0;
        } catch (IOException e) {
            logger.error("Can't contact the search server", e);
            return 0;
        }
    }

    @Override
    public boolean isExcludeUserSites() {
        return ignoreUserSites;
    }

    @Override
    public boolean isOnlyIndexSearchToolSites() {
        return searchToolRequired;
    }

    @Override
    public List<SearchBuilderItem> getGlobalMasterSearchItems() {
        //TODO: Don't return any item now as the indexing is handled by solr
        return null;
    }

    @Override
    public List<SearchBuilderItem> getAllSearchItems() {
        //TODO: Don't return any item now as the indexing is handled by solr
        return null;
    }

    @Override
    public List<SearchBuilderItem> getSiteMasterSearchItems() {
        //TODO: Don't return any item now as the indexing is handled by solr
        return null;
    }

    public static enum IndexAction {
        /**
         * Action Unknown, usually because the record has just been created
         */
        UNKNOWN(SearchBuilderItem.ACTION_UNKNOWN),

        /**
         * Action ADD the record to the search engine, if the doc ID is set, then
         * remove first, if not set, check its not there.
         */
        ADD(SearchBuilderItem.ACTION_ADD),

        /**
         * Action DELETE the record from the search engine, once complete delete the
         * record
         */
        DELETE(SearchBuilderItem.ACTION_DELETE),

        /**
         * The action REBUILD causes the indexer thread to rebuild the index from
         * scratch, re-fetching all entities This should only ever appear on the
         * master record
         */
        REBUILD(SearchBuilderItem.ACTION_REBUILD),

        /**
         * The action REFRESH causes the indexer thread to refresh the search index
         * from the current set of entities. If a Rebuild is in progress, the
         * refresh will not override the rebuild
         */
        REFRESH(SearchBuilderItem.ACTION_REFRESH);

        private final int itemAction;

        private IndexAction(int itemAction) {
            this.itemAction = itemAction;
        }

        public static IndexAction getAction(int itemActionId) {
            for (IndexAction indexAction : values()) {
                if (indexAction.getItemAction() == itemActionId)
                    return indexAction;
            }

            return null;
        }

        public int getItemAction() {
            return itemAction;
        }
    }

    public void setSiteService(SiteService siteService) {
        this.siteService = siteService;
    }

    public void setSolrServer(SolrServer solrServer) {
        this.solrServer = solrServer;
    }

    public void setSearchToolRequired(boolean searchToolRequired) {
        this.searchToolRequired = searchToolRequired;
    }

    public void setIgnoreUserSites(boolean ignoreUserSites) {
        this.ignoreUserSites = ignoreUserSites;
    }

    public void setContentProducerFactory(ContentProducerFactory contentProducerFactory) {
        this.contentProducerFactory = contentProducerFactory;
    }

    public void setIndexingExecutor(Executor indexingExecutor) {
        this.indexingExecutor = indexingExecutor;
    }
}
