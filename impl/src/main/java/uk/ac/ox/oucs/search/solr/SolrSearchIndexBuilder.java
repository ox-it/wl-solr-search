package uk.ac.ox.oucs.search.solr;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.util.NamedList;
import org.sakaiproject.event.api.Event;
import org.sakaiproject.event.api.Notification;
import org.sakaiproject.exception.IdUnusedException;
import org.sakaiproject.search.api.EntityContentProducer;
import org.sakaiproject.search.api.SearchIndexBuilder;
import org.sakaiproject.search.model.SearchBuilderItem;
import org.sakaiproject.site.api.SiteService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ox.oucs.search.indexing.Task;
import uk.ac.ox.oucs.search.producer.ContentProducerFactory;
import uk.ac.ox.oucs.search.queueing.DefaultTask;
import uk.ac.ox.oucs.search.queueing.IndexQueueing;
import uk.ac.ox.oucs.search.solr.indexing.SolrTools;
import uk.ac.ox.oucs.search.solr.util.AdminStatRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author Colin Hebert
 */
public class SolrSearchIndexBuilder implements SearchIndexBuilder {
    public static final String SEARCH_TOOL_ID = "sakai.search";
    private static final Logger logger = LoggerFactory.getLogger(SolrSearchIndexBuilder.class);
    private SiteService siteService;
    private SolrTools solrTools;
    private ContentProducerFactory contentProducerFactory;
    private boolean searchToolRequired;
    private boolean ignoreUserSites;
    private IndexQueueing indexQueueing;

    @Override
    public void addResource(Notification notification, Event event) {
        try {
            processEvent(event);
        } catch (Exception e) {
            logger.error("Event handling failed (this should NEVER happen)", e);
        }
    }

    private void processEvent(Event event) {
        String resourceName = event.getResource();
        logger.debug("Attempt to add or remove a resource from the index '" + resourceName + "'");
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
                    logger.debug("Impossible to index the content of the site '" + siteId + "' because the search tool hasn't been added");
                    return;
                }
            } catch (IdUnusedException e) {
                logger.warn("Couldn't find the site '" + siteId + "'", e);
                return;
            }
        }

        Task task;
        switch (entityContentProducer.getAction(event)) {
            case 1: //SearchBuilderItem.ACTION_ADD
                task = new DefaultTask(DefaultTask.Type.INDEX_DOCUMENT, event.getEventTime())
                        .setProperty(DefaultTask.RESOURCE_NAME, resourceName);
                break;
            case 2: //SearchBuilderItem.ACTION_DELETE
                task = new DefaultTask(DefaultTask.Type.REMOVE_DOCUMENT, event.getEventTime())
                        .setProperty(DefaultTask.RESOURCE_NAME, resourceName);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported action " + entityContentProducer.getAction(event) + " is not yet supported");
        }
        logger.debug("Add the task '" + task + "' to the queuing system");
        indexQueueing.addTaskToQueue(task);
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
        Task task = new DefaultTask(DefaultTask.Type.REFRESH_SITE)
                .setProperty(DefaultTask.SITE_ID, currentSiteId);
        logger.debug("Add the task '" + task + "' to the queuing system");
        indexQueueing.addTaskToQueue(task);
    }

    @Override
    public void rebuildIndex(String currentSiteId) {
        Task task = new DefaultTask(DefaultTask.Type.INDEX_SITE)
                .setProperty(DefaultTask.SITE_ID, currentSiteId);
        logger.debug("Add the task '" + task + "' to the queuing system");
        indexQueueing.addTaskToQueue(task);
    }

    @Override
    public void refreshIndex() {
        Task task = new DefaultTask(DefaultTask.Type.REFRESH_ALL);
        logger.debug("Add the task '" + task + "' to the queuing system");
        indexQueueing.addTaskToQueue(task);
    }

    @Override
    public boolean isBuildQueueEmpty() {
        return getPendingDocuments() == 0;
    }

    @Override
    public void rebuildIndex() {
        Task task = new DefaultTask(DefaultTask.Type.INDEX_ALL);
        logger.debug("Add the task '" + task + "' to the queuing system");
        indexQueueing.addTaskToQueue(task);
    }

    @Override
    public void destroy() {
        //TODO: Nope, we don't kill the search that easily
    }

    @Override
    public int getPendingDocuments() {
        return solrTools.getPendingDocuments();
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

    public void setSiteService(SiteService siteService) {
        this.siteService = siteService;
    }

    public void setSolrTools(SolrTools solrTools) {
        this.solrTools = solrTools;
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

    public void setIndexQueueing(IndexQueueing indexQueueing) {
        this.indexQueueing = indexQueueing;
    }
}
