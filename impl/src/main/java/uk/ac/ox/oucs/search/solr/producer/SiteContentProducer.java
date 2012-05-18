package uk.ac.ox.oucs.search.solr.producer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.sakaiproject.component.api.ServerConfigurationService;
import org.sakaiproject.entity.api.*;
import org.sakaiproject.event.api.Event;
import org.sakaiproject.exception.IdUnusedException;
import org.sakaiproject.search.api.EntityContentProducer;
import org.sakaiproject.search.api.SearchIndexBuilder;
import org.sakaiproject.search.api.SearchService;
import org.sakaiproject.search.api.SearchUtils;
import org.sakaiproject.search.model.SearchBuilderItem;
import org.sakaiproject.site.api.Site;
import org.sakaiproject.site.api.SiteService;

import java.io.Reader;
import java.io.StringReader;
import java.util.*;

/**
 * @author Colin Hebert
 */
public class SiteContentProducer implements EntityContentProducer {

    private static final Log log = LogFactory.getLog(SiteContentProducer.class);
    private EntityManager entityManager;
    private Collection<String> addEvents;
    private Collection<String> removeEvents;
    private SiteService siteService;
    private ServerConfigurationService serverConfigurationService;
    private SearchService searchService;
    private SearchIndexBuilder searchIndexBuilder;

    public void init() {
        addEvents = Arrays.asList(
                SiteService.SECURE_ADD_COURSE_SITE,
                SiteService.SECURE_ADD_SITE,
                SiteService.SECURE_ADD_USER_SITE,
                SiteService.SECURE_UPDATE_GROUP_MEMBERSHIP,
                SiteService.SECURE_UPDATE_SITE,
                SiteService.SECURE_UPDATE_SITE_MEMBERSHIP);
        removeEvents = Collections.singleton(SiteService.SECURE_REMOVE_SITE);

        if (serverConfigurationService.getBoolean("search.enable", false)) {
            for (String addEvent : addEvents) {
                searchService.registerFunction(addEvent);
            }
            for (String removeEvent : removeEvents) {
                searchService.registerFunction(removeEvent);
            }
            searchIndexBuilder.registerEntityContentProducer(this);
        }
    }

    public boolean canRead(String reference) {
        Reference ref = entityManager.newReference(reference);
        EntityProducer ep = ref.getEntityProducer();
        if (ep instanceof SiteService) {
            try {
                ((SiteService) ep).getSite(ref.getId());
                return true;
            } catch (Exception ex) {
                log.debug(ex);
            }
        }
        return false;
    }

    private Reference getReference(String reference) {
        return entityManager.newReference(reference);
    }

    public Integer getAction(Event event) {
        String evt = event.getEvent();
        if (evt == null) return SearchBuilderItem.ACTION_UNKNOWN;
        for (String match : addEvents) {
            if (evt.equals(match)) {
                return SearchBuilderItem.ACTION_ADD;
            }
        }
        for (String match : removeEvents) {
            if (evt.equals(match)) {
                return SearchBuilderItem.ACTION_DELETE;
            }
        }
        return SearchBuilderItem.ACTION_UNKNOWN;
    }

    public String getContainer(String ref) {
        // the site document is contined by itself
        return entityManager.newReference(ref).getId();

    }

    public String getContent(String reference) {
        Reference ref = entityManager.newReference(reference);
        EntityProducer ep = ref.getEntityProducer();
        if (ep instanceof SiteService) {
            try {
                Site site = ((SiteService) ep).getSite(ref.getId());
                return site.getTitle() + " " + site.getShortDescription() + " " + site.getDescription();

            } catch (IdUnusedException e) {
                throw new RuntimeException(" Failed to get message content ", e); //$NON-NLS-1$
            }
        }

        throw new RuntimeException(" Not a Message Entity " + reference); //$NON-NLS-1$

    }

    public Reader getContentReader(String reference) {
        return new StringReader(getContent(reference));
    }

    public Map<String, ?> getCustomProperties(String ref) {
        Map<String, Collection<String>> props = new HashMap<String, Collection<String>>();
        ResourceProperties rp = entityManager.newReference(ref).getEntity().getProperties();
        for (Iterator<String> i = rp.getPropertyNames(); i.hasNext(); ) {
            String key = i.next();
            props.put(key, rp.getPropertyList(key));
        }
        return props;
    }

    public String getCustomRDF(String ref) {
        return null;
    }

    public String getId(String ref) {
        return entityManager.newReference(ref).getId();
    }

    public Iterator<String> getSiteContentIterator(String context) {
        try {
            return Collections.singletonList(siteService.getSite(context).getReference()).iterator();
        } catch (IdUnusedException idu) {
            log.debug("Site Not Found for context " + context, idu);
            return Collections.<String>emptyList().iterator();
        }
    }

    public String getSiteId(String ref) {
        // this is the site that the document is visible to,
        // we need to look at the state of the site, and use special sites.
        // INFO: this is using not standard scoping that might want to be
        // reflected elsewhere
        Entity entity = entityManager.newReference(ref).getEntity();
        if (entity instanceof Site) {
            Site s = (Site) entity;
            if (s.isPublished() && s.isPubView()) {
                return ".auth";
            } else if (s.isPublished() && s.isJoinable()) {
                return ".anon";
            } else {
                // make unjoinable sites as private
                return ".private";
            }
        }
        return null;

    }

    public String getSubType(String ref) {
        return "";
    }

    public String getTitle(String ref) {
        Site s = (Site) entityManager.newReference(ref).getEntity();
        return SearchUtils.appendCleanString(s.getTitle(), null).toString();
    }


    public String getTool() {
        return "site";
    }

    public String getType(String ref) {
        return entityManager.newReference(ref).getType();
    }

    public String getUrl(String ref) {
        return entityManager.newReference(ref).getUrl();
    }

    public boolean isContentFromReader(String reference) {
        return false;
    }

    public boolean isForIndex(String ref) {
        Site s = (Site) entityManager.newReference(ref).getEntity();
        //SAK-18545 its possible the site no longer exits
        return s != null && s.isPublished();
    }

    public boolean matches(String ref) {
        EntityProducer ecp = entityManager.newReference(ref).getEntityProducer();
        return ecp instanceof SiteService;
    }

    public boolean matches(Event event) {
        return addEvents.contains(event.getEvent()) || removeEvents.contains(event.getEvent());
    }
}
