package uk.ac.ox.oucs.search.solr.producer;

import org.sakaiproject.content.api.ContentResource;
import org.sakaiproject.exception.IdUnusedException;
import org.sakaiproject.search.api.StoredDigestContentProducer;

import java.io.InputStream;
import java.io.Reader;
import java.util.List;

/**
 * @author Colin Hebert
 */
public class BinaryContentHostingContentProducer extends ContentHostingContentProducer implements BinaryEntityContentProducer, StoredDigestContentProducer {
    private List<String> supportedContentTypes;

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

    protected boolean isResourceTypeSupported(String contentType) {
        return supportedContentTypes.contains(contentType);
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


    public void setSupportedContentTypes(List<String> supportedContentTypes) {
        this.supportedContentTypes = supportedContentTypes;
    }
}
