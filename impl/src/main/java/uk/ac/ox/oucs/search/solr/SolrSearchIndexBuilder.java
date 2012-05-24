package uk.ac.ox.oucs.search.solr;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.ContentStreamBase;
import org.sakaiproject.event.api.Event;
import org.sakaiproject.event.api.Notification;
import org.sakaiproject.search.api.EntityContentProducer;
import org.sakaiproject.search.api.SearchIndexBuilder;
import org.sakaiproject.search.api.SearchService;
import org.sakaiproject.search.model.SearchBuilderItem;
import org.sakaiproject.site.api.Site;
import org.sakaiproject.site.api.SiteService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ox.oucs.search.solr.producer.BinaryEntityContentProducer;
import uk.ac.ox.oucs.search.solr.util.UpdateRequestReader;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * @author Colin Hebert
 */
public class SolrSearchIndexBuilder implements SearchIndexBuilder {
    public static final String COMMON_TOOL_ID = "sakai.search";
    public static final String LITERAL = "literal.";
    public static final String PROPERTY_PREFIX = "property_";
    public static final String UPREFIX = PROPERTY_PREFIX+"tika_";
    public static final String SOLRCELL_PATH = "/update/extract";
    private final Logger logger = LoggerFactory.getLogger(SolrSearchIndexBuilder.class);
    private final Collection<EntityContentProducer> entityContentProducers = new HashSet<EntityContentProducer>();
    private SiteService siteService;
    private SolrServer solrServer;
    private boolean searchToolRequired;
    private boolean ignoreUserSites;

    @Override
    public void addResource(Notification notification, Event event) {
        logger.debug("Attempt to add or remove a resource from the index");
        String resourceName = event.getResource();
        //Set the resource name to empty instead of null
        if (resourceName == null)
            resourceName = "";

        EntityContentProducer entityContentProducer = newEntityContentProducer(event);
        //If there is no matching entity content producer or no associated site, return
        if (entityContentProducer == null || entityContentProducer.getSiteId(resourceName) == null) {
            logger.debug("Can't find an entityContentProducer for '" + resourceName + "'");
            return;
        }

        String siteId = entityContentProducer.getSiteId(resourceName);

        //If the indexing is only enabled on sites with search tool, check that the tool is actually enabled
        if (isOnlyIndexSearchToolSites()) {
            try {
                if (siteService.getSite(siteId).getToolForCommonId(COMMON_TOOL_ID) == null)
                    return;
            } catch (Exception ex) {
                logger.debug("Can't index content if the search tool isn't activated. Site: " + siteId);
                return;
            }
        }

        try {
            ItemAction action = ItemAction.getAction(entityContentProducer.getAction(event));
            logger.debug("Action on '" + resourceName + "' detected as " + action.name());
            SolrRequest request;
            switch (action) {
                case ADD:
                    request = toSolrRequest(resourceName, entityContentProducer);
                    break;
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

    @Override
    public void registerEntityContentProducer(EntityContentProducer ecp) {
        logger.info(ecp.getClass() + " registered to provide content to the search index from " + ecp.getTool());
        entityContentProducers.add(ecp);
    }

    @Override
    public EntityContentProducer newEntityContentProducer(String ref) {
        for (EntityContentProducer ecp : entityContentProducers) {
            if (ecp.matches(ref)) {
                return ecp;
            }
        }
        return null;
    }

    @Override
    public EntityContentProducer newEntityContentProducer(Event event) {
        for (EntityContentProducer ecp : entityContentProducers) {
            if (ecp.matches(event)) {
                return ecp;
            }
        }
        return null;
    }

    @Override
    public List<EntityContentProducer> getContentProducers() {
        return new ArrayList<EntityContentProducer>(entityContentProducers);
    }

    @Override
    public void refreshIndex(String currentSiteId) {
        logger.info("Refreshing the index for '" + currentSiteId + "'");
        try {
            Collection<String> resourceNames = getResourceNames(currentSiteId);
            logger.info(resourceNames.size() + " elements will be refreshed");
            removeSiteIndexContent(currentSiteId);
            for (String resourceName : resourceNames) {
                EntityContentProducer entityContentProducer = newEntityContentProducer(resourceName);

                //If there is no matching entity content producer or no associated site, skip the resource
                //it is either not available anymore, or the corresponding entityContentProducer doesn't exist anymore
                if (entityContentProducer == null || entityContentProducer.getSiteId(resourceName) == null) {
                    logger.info("Couldn't either find an entityContentProducer or the resource itself for '" + resourceName + "'");
                    continue;
                }

                try {
                    solrServer.request(toSolrRequest(resourceName, entityContentProducer));
                } catch (Exception e) {
                    logger.warn("Unexpected exception while preparing the solr request for '" + resourceName + "'", e);
                }
            }

            solrServer.commit();
        } catch (SolrServerException e) {
            logger.warn("Couldn't refresh the index for site '" + currentSiteId + "'", e);
        } catch (IOException e) {
            logger.error("Couln't access the solr server", e);
        }
    }

    private Collection<String> getResourceNames(String currentSiteId) {
        try {
            logger.debug("Obtaining indexed elements for site: '" + currentSiteId + "'");
            SolrQuery query = new SolrQuery()
                    .setQuery(SearchService.FIELD_SITEID + ':' + currentSiteId)
                    .addField(SearchService.FIELD_REFERENCE);
            SolrDocumentList results = solrServer.query(query).getResults();
            Collection<String> resourceNames = new ArrayList<String>(results.size());
            for (SolrDocument document : results) {
                resourceNames.add((String) document.getFieldValue(SearchService.FIELD_REFERENCE));
            }
            return resourceNames;
        } catch (SolrServerException e) {
            logger.warn("Couldn't get indexed elements for site: '" + currentSiteId + "'", e);
            return Collections.emptyList();
        }
    }

    @Override
    public void rebuildIndex(final String currentSiteId) {
        logger.info("Rebuilding the index for '" + currentSiteId + "'");

        removeSiteIndexContent(currentSiteId);
        for (final EntityContentProducer entityContentProducer : getContentProducers()) {
            try {
                Iterable<String> resourceNames = new Iterable<String>() {
                    @Override
                    public Iterator<String> iterator() {
                        return entityContentProducer.getSiteContentIterator(currentSiteId);
                    }
                };

                for (String resourceName : resourceNames) {
                    try {
                        solrServer.request(toSolrRequest(resourceName, entityContentProducer));
                    } catch (Exception e) {
                        logger.warn("Unexpected exception while preparing the solr request for '" + resourceName + "'", e);
                    }
                }

                solrServer.commit();
            } catch (SolrServerException e) {
                logger.warn("Couldn't rebuild the index for site '" + currentSiteId + "'", e);
            } catch (IOException e) {
                logger.error("Couln't access the solr server", e);
            }
        }
    }

    private void removeSiteIndexContent(String currentSiteId) {
        logger.info("Removing content for site '" + currentSiteId + "'");
        try {
            solrServer.request(new UpdateRequest().deleteByQuery(SearchService.FIELD_SITEID + ':' + currentSiteId));
            solrServer.commit();
        } catch (SolrServerException e) {
            logger.warn("Couldn't clean the index for site '" + currentSiteId + "'", e);
        } catch (IOException e) {
            logger.error("Couln't access the solr server", e);
        }
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
        for (Site s : siteService.getSites(SiteService.SelectionType.ANY, null, null, null, SiteService.SortType.NONE, null)) {
            if (isSiteIndexable(s)) {
                rebuildIndex(s.getId());
            }
        }
    }

    private boolean isSiteIndexable(Site s) {
        logger.debug("Check if '" + s.getId() + "' is indexable.");
        // Do not index:
        //  - Special sites
        //  - Sites without the search tool (if the option is enabled)
        //  - User sites (if the option is enabled)
        return !(siteService.isSpecialSite(s.getId()) ||
                (isOnlyIndexSearchToolSites() && s.getToolForCommonId("sakai.search") == null) ||
                (isExcludeUserSites() && siteService.isUserSite(s.getId())));
    }

    @Override
    public void destroy() {
        //TODO: Nope, we don't kill the search that easily
    }

    @Override
    public int getPendingDocuments() {
        //TODO: If documents are handled by SolR we don't have any pending document
        return 0;
    }

    @Override
    public boolean isExcludeUserSites() {
        return ignoreUserSites;
    }

    /**
     * Enable indexing only on sites with the search tool enabled
     */
    @Override
    public boolean isOnlyIndexSearchToolSites() {
        return searchToolRequired;
    }

    @Override
    public List<SearchBuilderItem> getGlobalMasterSearchItems() {
        //TODO: Don't return any item now as the indexing is handled by solr
        return Collections.emptyList();
    }

    @Override
    public List<SearchBuilderItem> getAllSearchItems() {
        //TODO: Don't return any item now as the indexing is handled by solr
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<SearchBuilderItem> getSiteMasterSearchItems() {
        //TODO: Don't return any item now as the indexing is handled by solr
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public static enum ItemAction {
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

        private ItemAction(int itemAction) {
            this.itemAction = itemAction;
        }

        public static ItemAction getAction(int itemActionId) {
            for (ItemAction itemAction : values()) {
                if (itemAction.getItemAction() == itemActionId)
                    return itemAction;
            }

            return null;
        }

        public int getItemAction() {
            return itemAction;
        }
    }

    private SolrRequest toSolrRequest(final String resourceName, EntityContentProducer contentProducer) {
        logger.debug("Create a solr request to add '" + resourceName + "' to the index");
        SolrRequest request;
        SolrInputDocument document = new SolrInputDocument();

        //TODO: Solr handles dates, use that instead of a string timestamp...
        document.addField(SearchService.DATE_STAMP, String.valueOf(System.currentTimeMillis()));
        document.addField(SearchService.FIELD_CONTAINER, contentProducer.getContainer(resourceName));
        document.addField(SearchService.FIELD_ID, contentProducer.getId(resourceName));
        document.addField(SearchService.FIELD_TYPE, contentProducer.getType(resourceName));
        document.addField(SearchService.FIELD_SUBTYPE, contentProducer.getSubType(resourceName));
        document.addField(SearchService.FIELD_REFERENCE, resourceName);
        document.addField(SearchService.FIELD_CONTEXT, contentProducer.getSiteId(resourceName));
        document.addField(SearchService.FIELD_TITLE, contentProducer.getTitle(resourceName));
        document.addField(SearchService.FIELD_TOOL, contentProducer.getTool());
        document.addField(SearchService.FIELD_URL, contentProducer.getUrl(resourceName));
        document.addField(SearchService.FIELD_SITEID, contentProducer.getSiteId(resourceName));

        //Add the custom properties
        Map<String, Collection<String>> properties = extractCustomProperties(resourceName, contentProducer);
        for (Map.Entry<String, Collection<String>> entry : properties.entrySet()) {
            document.addField(PROPERTY_PREFIX + entry.getKey(), entry.getValue());
        }

        //Prepare the actual request based on a stream/reader/string
        if (contentProducer instanceof BinaryEntityContentProducer) {
            logger.debug("Create a SolrCell request");
            request = prepareSolrCellRequest(resourceName, (BinaryEntityContentProducer) contentProducer, document);
        } else if (contentProducer.isContentFromReader(resourceName)) {
            logger.debug("Create a request with a Reader");
            document.setField(SearchService.FIELD_CONTENTS, contentProducer.getContentReader(resourceName));
            request = new UpdateRequestReader().add(document);
        } else {
            logger.debug("Create a request based on a String");
            document.setField(SearchService.FIELD_CONTENTS, contentProducer.getContent(resourceName));
            request = new UpdateRequest().add(document);
        }

        return request;
    }

    /**
     * Prepare a request toward SolrCell to parse a binary document.
     * <p>
     * The given document will be send in its binary form to apache tika to be analysed and stored in the index.
     * </p>
     *
     * @param resourceName    name of the document
     * @param contentProducer associated content producer providing a binary stream of data
     * @param document        {@link SolrInputDocument} used to prepare index fields
     * @return a solrCell request
     */
    private SolrRequest prepareSolrCellRequest(final String resourceName, final BinaryEntityContentProducer contentProducer,
                                               SolrInputDocument document) {
        //Send to tika
        ContentStreamUpdateRequest contentStreamUpdateRequest = new ContentStreamUpdateRequest(SOLRCELL_PATH);
        contentStreamUpdateRequest.setParam("fmap.content", SearchService.FIELD_CONTENTS);
        contentStreamUpdateRequest.setParam("uprefix", UPREFIX);
        ContentStreamBase contentStreamBase = new ContentStreamBase() {
            @Override
            public InputStream getStream() throws IOException {
                return contentProducer.getContentStream(resourceName);
            }
        };
        contentStreamUpdateRequest.addContentStream(contentStreamBase);
        for (SolrInputField field : document) {
            contentStreamUpdateRequest.setParam("fmap.sakai_" + field.getName(), field.getName());
            for (Object o : field) {
                //The "sakai_" part is due to SOLR-3386, this fix should be temporary
                contentStreamUpdateRequest.setParam(LITERAL + "sakai_" + field.getName(), o.toString());
            }
        }
        return contentStreamUpdateRequest;
    }

    private Map<String, Collection<String>> extractCustomProperties(String resourceName, EntityContentProducer contentProducer) {
        Map<String, ?> m = contentProducer.getCustomProperties(resourceName);

        if (m == null)
            return Collections.emptyMap();

        Map<String, Collection<String>> properties = new HashMap<String, Collection<String>>(m.size());
        for (Map.Entry<String, ?> propertyEntry : m.entrySet()) {
            String propertyName = toSolrFieldName(propertyEntry.getKey());
            Object propertyValue = propertyEntry.getValue();
            Collection<String> values;

            if (propertyValue instanceof String)
                values = Collections.singleton((String) propertyValue);
            else if (propertyValue instanceof String[])
                values = Arrays.asList((String[]) propertyValue);
            else if (propertyValue instanceof Collection)
                values = (Collection<String>) propertyValue;
            else {
                if (propertyValue != null)
                    logger.warn("Couldn't find what the value for '" + propertyName + "' was. It has been ignored. " + propertyName.getClass());
                values = Collections.emptyList();
            }

            if (properties.containsKey(propertyName)) {
                logger.warn("Two properties had a really similar name and were merged. This shouldn't happen! " + propertyName);
                values = new ArrayList<String>(values);
                values.addAll(properties.get(propertyName));
            }

            properties.put(propertyName, values);
        }

        return properties;
    }

    /**
     * Replace special characters, turn to lower case and avoid repetitive '_'
     *
     * @param propertyName String to filter
     * @return a filtered name more appropriate to use with solr
     */
    private String toSolrFieldName(String propertyName) {
        StringBuilder sb = new StringBuilder(propertyName.length());
        boolean lastUnderscore = false;
        for (Character c : propertyName.toLowerCase().toCharArray()) {
            if ((c < 'a' || c > 'z') && (c < '0' || c > '9'))
                c = '_';
            if (!lastUnderscore || c != '_')
                sb.append(c);
            lastUnderscore = (c == '_');
        }
        logger.debug("Transformed the '" + propertyName + "' property into: '" + sb + "'");
        return sb.toString();
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
}
