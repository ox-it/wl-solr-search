package uk.ac.ox.oucs.search.solr;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
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
import org.sakaiproject.tool.api.Session;
import org.sakaiproject.tool.api.SessionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ox.oucs.search.solr.process.CleanSiteIndexProcess;
import uk.ac.ox.oucs.search.solr.process.IndexDocumentProcess;
import uk.ac.ox.oucs.search.solr.util.AdminStatRequest;

import java.io.IOException;
import java.util.*;
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
    private SessionManager sessionManager;
    private boolean searchToolRequired;
    private boolean ignoreUserSites;
    private Executor indexingExecutor;

    @Override
    public void addResource(Notification notification, Event event) {
        logger.debug("Attempt to add or remove a resource from the index");
        String resourceName = event.getResource();
        //Set the resource name to empty instead of null
        if (resourceName == null)
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
        indexingExecutor.execute(new DocumentIndexer(entityContentProducer, action, resourceName));
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
        indexingExecutor.execute(new SiteIndexRefresher(currentSiteId));
    }

    /**
     * Get all indexed resources for a site
     *
     * @param siteId Site containing indexed resources
     * @return a collection of resource references or an empty collection if no resource was found
     */
    private Collection<String> getResourceNames(String siteId) {
        try {
            logger.debug("Obtaining indexed elements for site: '" + siteId + "'");
            SolrQuery query = new SolrQuery()
                    .setQuery(SearchService.FIELD_SITEID + ':' + siteId)
                    .addField(SearchService.FIELD_REFERENCE);
            SolrDocumentList results = solrServer.query(query).getResults();
            Collection<String> resourceNames = new ArrayList<String>(results.size());
            for (SolrDocument document : results) {
                resourceNames.add((String) document.getFieldValue(SearchService.FIELD_REFERENCE));
            }
            return resourceNames;
        } catch (SolrServerException e) {
            logger.warn("Couldn't get indexed elements for site: '" + siteId + "'", e);
            return Collections.emptyList();
        }
    }

    @Override
    public void rebuildIndex(final String currentSiteId) {
        indexingExecutor.execute(new SiteIndexBuilder(currentSiteId));
    }

    /**
     * Remove every indexed content belonging to a site
     *
     * @param siteId indexed site
     */
    private void cleanSiteIndex(String siteId) {
        new CleanSiteIndexProcess(solrServer, siteId).execute();
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

    public void setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    /**
     * Runnable class handling the indexation or removal of one document
     */
    private class DocumentIndexer implements Runnable {
        private final EntityContentProducer entityContentProducer;
        private final IndexAction action;
        private final String resourceName;

        public DocumentIndexer(EntityContentProducer entityContentProducer, IndexAction action, String resourceName) {
            this.entityContentProducer = entityContentProducer;
            this.action = action;
            this.resourceName = resourceName;
        }

        @Override
        public void run() {
            try {
                logger.debug("Action on '" + resourceName + "' detected as " + action.name());
                setCurrentSessionUserAdmin();
                SolrRequest request;
                switch (action) {
                    case ADD:
                        new IndexDocumentProcess(solrServer, entityContentProducer, resourceName).execute();
                        return;
                    case DELETE:
                        request = new UpdateRequest().deleteById(entityContentProducer.getId(resourceName));
                        break;
                    default:
                        throw new UnsupportedOperationException(action + " is not yet supported");
                }
                solrServer.request(request);
                solrServer.commit();
            } catch (SolrServerException e) {
                logger.warn("Couldn't execute the request", e);
            } catch (IOException e) {
                logger.error("Can't contact the search server", e);
            }
        }
    }

    /**
     * Runnable class refreshing one site's index
     */
    private class SiteIndexRefresher implements Runnable {
        private final String siteId;

        public SiteIndexRefresher(String siteId) {
            this.siteId = siteId;
        }

        @Override
        public void run() {
            logger.info("Refreshing the index for '" + siteId + "'");
            setCurrentSessionUserAdmin();
            try {
                //Get the currently indexed resources for this site
                Collection<String> resourceNames = getResourceNames(siteId);
                logger.info(resourceNames.size() + " elements will be refreshed");
                cleanSiteIndex(siteId);
                for (String resourceName : resourceNames) {
                    EntityContentProducer entityContentProducer = contentProducerFactory.getContentProducerForElement(resourceName);

                    //If there is no matching entity content producer or no associated site, skip the resource
                    //it is either not available anymore, or the corresponding entityContentProducer doesn't exist anymore
                    if (entityContentProducer == null || entityContentProducer.getSiteId(resourceName) == null) {
                        logger.info("Couldn't either find an entityContentProducer or the resource itself for '" + resourceName + "'");
                        continue;
                    }

                    new IndexDocumentProcess(solrServer, entityContentProducer, resourceName, false).execute();
                }

                solrServer.commit();
            } catch (SolrServerException e) {
                logger.warn("Couldn't refresh the index for site '" + siteId + "'", e);
            } catch (IOException e) {
                logger.error("Can't contact the search server", e);
            }
        }
    }

    /**
     * Runnable class handling one site re-indexation
     */
    private class SiteIndexBuilder implements Runnable {
        private final String siteId;

        public SiteIndexBuilder(String siteId) {
            this.siteId = siteId;
        }

        @Override
        public void run() {
            logger.info("Rebuilding the index for '" + siteId + "'");
            setCurrentSessionUserAdmin();
            new CleanSiteIndexProcess(solrServer, siteId).execute();
            for (final EntityContentProducer entityContentProducer : contentProducerFactory.getContentProducers()) {
                try {
                    Iterable<String> resourceNames = new Iterable<String>() {
                        @Override
                        public Iterator<String> iterator() {
                            return entityContentProducer.getSiteContentIterator(siteId);
                        }
                    };

                    for (String resourceName : resourceNames) {
                        new IndexDocumentProcess(solrServer, entityContentProducer, resourceName).execute();
                    }

                    solrServer.commit();
                } catch (SolrServerException e) {
                    logger.warn("Couldn't rebuild the index for site '" + siteId + "'", e);
                } catch (IOException e) {
                    logger.error("Can't contact the search server", e);
                }

            }
        }
    }

    private void setCurrentSessionUserAdmin() {
        Session session = sessionManager.getCurrentSession();
        session.setUserId("admin");
        session.setUserEid("admin");
    }
}
