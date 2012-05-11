package uk.ac.ox.oucs.search.solr.producer;

import org.sakaiproject.component.api.ServerConfigurationService;
import org.sakaiproject.content.api.ContentHostingService;
import org.sakaiproject.content.api.ContentResource;
import org.sakaiproject.entity.api.EntityManager;
import org.sakaiproject.entity.api.ResourceProperties;
import org.sakaiproject.event.api.Event;
import org.sakaiproject.exception.IdUnusedException;
import org.sakaiproject.search.api.SearchIndexBuilder;
import org.sakaiproject.search.api.SearchService;
import org.sakaiproject.search.api.StoredDigestContentProducer;
import uk.ac.ox.oucs.search.solr.SolrSearchIndexBuilder;

import java.io.InputStream;
import java.io.Reader;
import java.util.*;

/**
 * @author Colin Hebert
 */
public class BinaryContentHostingContentProducer implements BinaryEntityContentProducer, StoredDigestContentProducer {
    private ServerConfigurationService serverConfigurationService;
    private SearchService searchService;
    private SearchIndexBuilder searchIndexBuilder;
    private ContentHostingService contentHostingService;
    private EntityManager entityManager;

    public void init() {
        if (serverConfigurationService.getBoolean("search.enable", false)) {
            searchService.registerFunction(ContentHostingService.EVENT_RESOURCE_ADD);
            searchService.registerFunction(ContentHostingService.EVENT_RESOURCE_WRITE);
            searchService.registerFunction(ContentHostingService.EVENT_RESOURCE_REMOVE);
            searchIndexBuilder.registerEntityContentProducer(this);
        }
    }

    @Override
    public boolean isContentFromReader(String reference) {
        return false;
    }

    @Override
    public Reader getContentReader(String reference) {
        return null;
    }

    @Override
    public String getContent(String reference) {
        return null;
    }

    @Override
    public String getTitle(String reference) {
        ContentResource contentResource;
        try {
            contentResource = contentHostingService.getResource(getId(reference));
        } catch (Exception e) {
            throw new RuntimeException("Failed to resolve resource ", e);
        }
        ResourceProperties rp = contentResource.getProperties();
        String displayNameProp = rp.getNamePropDisplayName();
        return rp.getProperty(displayNameProp);
    }

    @Override
    public Integer getAction(Event event) {
        String eventName = event.getEvent();
        if ((ContentHostingService.EVENT_RESOURCE_ADD.equals(eventName)
                || ContentHostingService.EVENT_RESOURCE_WRITE.equals(eventName)) && isForIndex(event.getResource())) {
            return SolrSearchIndexBuilder.ItemAction.ADD.getItemAction();
        } else if (ContentHostingService.EVENT_RESOURCE_REMOVE.equals(eventName) && isForIndexDelete(event.getResource())) {
            return SolrSearchIndexBuilder.ItemAction.DELETE.getItemAction();
        } else {
            return SolrSearchIndexBuilder.ItemAction.UNKNOWN.getItemAction();
        }
    }

    @Override
    public boolean matches(Event event) {
        return SolrSearchIndexBuilder.ItemAction.UNKNOWN.getItemAction() != getAction(event);
    }

    @Override
    public String getTool() {
        return "content";
    }

    @Override
    public Iterator<String> getSiteContentIterator(String context) {
        String siteCollection = contentHostingService.getSiteCollection(context);
        final Iterable<ContentResource> siteContent;
        if (!"/".equals(siteCollection)) siteContent = contentHostingService.getAllResources(siteCollection);
        else siteContent = Collections.emptyList();

        return new Iterator<String>() {
            Iterator<ContentResource> scIterator = siteContent.iterator();

            public boolean hasNext() {
                return scIterator.hasNext();
            }

            public String next() {
                return scIterator.next().getReference();
            }

            public void remove() {
                throw new UnsupportedOperationException("Remove is not implemented ");
            }
        };
    }

    /**
     * nasty hack to not index dropbox without loading an entity from the DB
     */
    private boolean isInDropbox(String reference) {
        return reference.length() > "/content".length() && contentHostingService.isInDropbox(reference.substring("/content".length()));
    }

    private boolean isAnAssignment(String reference) {
        String[] parts = reference.split("/");
        return parts.length > 4 && "Assignments".equals(parts[4]) && ContentHostingService.ATTACHMENTS_COLLECTION.equals("/" + parts[2] + "/");
    }

    private boolean isForIndexDelete(String reference) {
        return !isInDropbox(reference);
    }

    @Override
    public boolean isForIndex(String reference) {
        try {
            if (isInDropbox(reference) || isAnAssignment(reference))
                return false;

            ContentResource contentResource = contentHostingService.getResource(getId(reference));

            //Only index files, not directories
            return !(contentResource == null || contentResource.isCollection());
        } catch (IdUnusedException idun) {
            return false; // an unknown resource that cant be indexed
        } catch (Exception e) {
            throw new RuntimeException("Failed to resolve resource ", e);
        }
    }

    @Override
    public boolean canRead(String reference) {
        try {
            contentHostingService.checkResource(getId(reference));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public Map<String, ?> getCustomProperties(String ref) {
        try {
            ContentResource contentResource;
            contentResource = contentHostingService.getResource(getId(ref));

            Map<String, String[]> cp = new HashMap<String, String[]>();

            Iterator<String> propertiesIterator = contentResource.getProperties().getPropertyNames();

            while (propertiesIterator.hasNext()) {
                String propertyName = propertiesIterator.next();
                List<String> prop = contentResource.getProperties().getPropertyList(propertyName);
                if (prop != null) {
                    cp.put(propertyName, prop.toArray(new String[prop.size()]));
                }
            }
            return cp;
        } catch (Exception e) {
        }
        return Collections.emptyMap();
    }

    @Override
    public String getCustomRDF(String ref) {
        return null;
    }

    @Override
    public String getUrl(String reference) {
        return entityManager.newReference(reference).getUrl();
    }

    @Override
    public String getId(String ref) {
        return entityManager.newReference(ref).getId();
    }

    @Override
    public String getType(String ref) {
        return entityManager.newReference(ref).getType();
    }

    @Override
    public String getSubType(String ref) {
        return entityManager.newReference(ref).getSubType();
    }

    @Override
    public String getContainer(String ref) {
        return entityManager.newReference(ref).getContainer();
    }

    @Override
    public String getSiteId(String reference) {
        return entityManager.newReference(reference).getContext();
    }

    @Override
    public boolean matches(String reference) {
        return entityManager.newReference(reference).getEntityProducer() instanceof ContentHostingService;
    }

    @Override
    public InputStream getContentStream(String ref) {
        ContentResource contentResource;
        try {
            contentResource = contentHostingService.getResource(getId(ref));
        } catch (Exception e) {
            throw new RuntimeException("Failed to resolve resource " + ref, e);
        }

        InputStream stream;
        try {
            stream = contentResource.streamContent();
        } catch (Exception ex) {
            throw new RuntimeException("Failed to obtain content from " + ref, ex);
        }
        return stream;
    }

    public void setServerConfigurationService(ServerConfigurationService serverConfigurationService) {
        this.serverConfigurationService = serverConfigurationService;
    }

    public void setSearchService(SearchService searchService) {
        this.searchService = searchService;
    }

    public void setSearchIndexBuilder(SearchIndexBuilder searchIndexBuilder) {
        this.searchIndexBuilder = searchIndexBuilder;
    }

    public void setContentHostingService(ContentHostingService contentHostingService) {
        this.contentHostingService = contentHostingService;
    }

    public void setEntityManager(EntityManager entityManager) {
        this.entityManager = entityManager;
    }
}
