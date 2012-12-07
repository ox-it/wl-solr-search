package uk.ac.ox.oucs.search.producer;

import org.sakaiproject.search.api.EntityContentProducer;

import java.io.InputStream;

/**
 * Adds the possibility to load a binary stream from {@link EntityContentProducer}.
 * <p>
 * This can be useful when the content has to be digested and indexed at the same time.
 * </p>
 *
 * @author Colin Hebert
 */
public interface BinaryEntityContentProducer extends EntityContentProducer {
    InputStream getContentStream(String reference);
    String getContentType(String reference);
    String getResourceName(String reference);
}
