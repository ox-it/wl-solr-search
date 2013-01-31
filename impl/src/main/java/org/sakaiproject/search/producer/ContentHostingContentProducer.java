package org.sakaiproject.search.producer;

import org.sakaiproject.component.api.ServerConfigurationService;
import org.sakaiproject.content.api.ContentHostingService;
import org.sakaiproject.content.api.ContentResource;
import org.sakaiproject.entity.api.EntityManager;
import org.sakaiproject.entity.api.ResourceProperties;
import org.sakaiproject.event.api.Event;
import org.sakaiproject.exception.IdUnusedException;
import org.sakaiproject.search.api.EntityContentProducer;
import org.sakaiproject.search.api.SearchIndexBuilder;
import org.sakaiproject.search.api.SearchService;
import org.sakaiproject.search.model.SearchBuilderItem;

import java.util.*;

import static org.sakaiproject.content.api.ContentHostingService.EVENT_RESOURCE_ADD;
import static org.sakaiproject.content.api.ContentHostingService.EVENT_RESOURCE_REMOVE;
import static org.sakaiproject.content.api.ContentHostingService.EVENT_RESOURCE_WRITE;

/**
 * @author Colin Hebert
 */
public abstract class ContentHostingContentProducer implements EntityContentProducer {
    protected ServerConfigurationService serverConfigurationService;
    protected SearchService searchService;
    protected SearchIndexBuilder searchIndexBuilder;
    protected ContentHostingService contentHostingService;
    protected EntityManager entityManager;

    public void init() {
        if (serverConfigurationService.getBoolean("search.enable", false)) {
            searchService.registerFunction(EVENT_RESOURCE_ADD);
            searchService.registerFunction(EVENT_RESOURCE_WRITE);
            searchService.registerFunction(EVENT_RESOURCE_REMOVE);
            //TODO: Replace this with a registration on a factory
            searchIndexBuilder.registerEntityContentProducer(this);
        }
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
        //Skip the resourceType check if the event isn't about resources
        if(!EVENT_RESOURCE_REMOVE.equals(eventName) && !EVENT_RESOURCE_ADD.equals(eventName) && !EVENT_RESOURCE_WRITE.equals(eventName))
            return SearchBuilderItem.ACTION_UNKNOWN;

        String resourceType = getResourceType(event.getResource());
        //If the resource type isn't provided, assume that it's a document we want to delete, try to proceed.
        if(resourceType == null && EVENT_RESOURCE_REMOVE.equals(eventName) && isForIndexDelete(event.getResource())) {
            return SearchBuilderItem.ACTION_DELETE;
        } else if(isResourceTypeSupported(resourceType) &&
                (EVENT_RESOURCE_ADD.equals(eventName) || EVENT_RESOURCE_WRITE.equals(eventName)) &&
                isForIndex(event.getResource())){
            return SearchBuilderItem.ACTION_ADD;
        } else {
            return SearchBuilderItem.ACTION_UNKNOWN;
        }
    }

    private String getResourceType(String reference) {
        try {
            if (reference == null)
                return null;
            return contentHostingService.getResource(getId(reference)).getResourceType();
        } catch (IdUnusedException e) {
            return null;
        } catch (Exception e) {
            throw new RuntimeException("Failed to resolve resource ", e);
        }
    }

    protected abstract boolean isResourceTypeSupported(String contentType);

    @Override
    public boolean matches(Event event) {
        return SearchBuilderItem.ACTION_UNKNOWN != getAction(event);
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
            String nextReference;
            boolean hasNext = true;

            {
                checkForNext();
            }

            public boolean hasNext() {
                return hasNext;
            }

            public String next() {
                if (!hasNext)
                    throw new NoSuchElementException();

                String nextReference = this.nextReference;
                checkForNext();
                return nextReference;
            }

            public void remove() {
                throw new UnsupportedOperationException("Remove is not implemented ");
            }

            private void checkForNext() {
                while (scIterator.hasNext()) {
                    String reference = scIterator.next().getReference();
                    String resourceType = getResourceType(reference);
                    if (isResourceTypeSupported(resourceType)) {
                        nextReference = reference;
                        return;
                    }
                }
                hasNext = false;
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
            return contentResource != null && !contentResource.isCollection();
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
    public Map<String, Collection<String>> getCustomProperties(String ref) {
        try {
            Map<String, Collection<String>> props = new HashMap<String, Collection<String>>();

            ResourceProperties rp = contentHostingService.getResource(getId(ref)).getProperties();
            Iterator<String> propertiesIterator = rp.getPropertyNames();
            while (propertiesIterator.hasNext()) {
                String propertyName = propertiesIterator.next();
                props.put(propertyName, rp.getPropertyList(propertyName));
            }
            return props;
        } catch (Exception e) {
            return Collections.emptyMap();
        }
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