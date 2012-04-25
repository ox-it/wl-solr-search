package uk.ac.ox.oucs.search.solr.producer;

import org.sakaiproject.search.api.EntityContentProducer;

import java.io.InputStream;

/**
 * Adds the possibility to load a binary stream from {@link EntityContentProducer}.
 *
 * This can be useful when the content has to be digested and indexed at the same time.
 *
 * @author Colin Hebert
 */
public interface BinaryEntityContentProducer extends EntityContentProducer {
    InputStream getContentStream(String ref);
}
