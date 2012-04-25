package uk.ac.ox.oucs.search.solr.producer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.sakaiproject.content.api.ContentResource;
import org.sakaiproject.entity.api.Reference;
import org.sakaiproject.exception.IdUnusedException;
import org.sakaiproject.exception.PermissionException;
import org.sakaiproject.exception.TypeException;
import org.sakaiproject.search.component.adapter.contenthosting.ContentHostingContentProducer;

import java.io.InputStream;
import java.util.*;

/**
 * ContentHostingBinaryContentProducer provide the ability to get a binary stream from an hosted Content
 */
public class ContentHostingBinaryContentProducer extends ContentHostingContentProducer implements BinaryEntityContentProducer {
    private static Log log = LogFactory.getLog(ContentHostingContentProducer.class);

    @Override
    public Map<String, String[]> getCustomProperties(String ref) {
        try {
            Reference reference = getEntityManager().newReference(ref);
            ContentResource contentResource;
            contentResource = getContentHostingService().getResource(reference.getId());

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
        } catch (PermissionException e) {
            log.debug(e);
        } catch (IdUnusedException e) {
            log.debug(e);
        } catch (TypeException e) {
            log.debug(e);
        }
        return Collections.emptyMap();
    }

    @Override
    public InputStream getContentStream(String ref) {

        boolean debug = log.isDebugEnabled();
        ContentResource contentResource;
        try {
            Reference reference = getEntityManager().newReference(ref);
            contentResource = getContentHostingService().getResource(reference.getId());
        } catch (Exception e) {
            throw new RuntimeException("Failed to resolve resource " + ref, e);
        }

        InputStream stream ;
        try {
            stream = contentResource.streamContent();
        } catch (Exception ex) {
            throw new RuntimeException("Failed to obtain content from " + ref, ex);
        }
        if (debug) {
            log.debug("ContentHosting.getContentStrean" + ref + ":" + stream);
        }
        return stream;
    }
}
