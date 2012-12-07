package uk.ac.ox.oucs.search.producer;

import org.sakaiproject.content.api.ContentResource;
import org.sakaiproject.search.api.StoredDigestContentProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.util.List;

/**
 * @author Colin Hebert
 */
public class BinaryContentHostingContentProducer extends ContentHostingContentProducer implements BinaryEntityContentProducer, StoredDigestContentProducer {
    private static final Logger logger = LoggerFactory.getLogger(BinaryContentHostingContentProducer.class);
    private static final byte[] EMPTY_DOCUMENT = new byte[0];
    private List<String> supportedResourceTypes;
    private long documentMaximumSize = Long.MAX_VALUE;

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

    protected boolean isResourceTypeSupported(String resourceType) {
        return supportedResourceTypes.contains(resourceType);
    }

    @Override
    public InputStream getContentStream(String reference) {
        ContentResource contentResource;
        try {
            contentResource = contentHostingService.getResource(getId(reference));

            if (contentResource.getContentLength() > documentMaximumSize) {
                logger.info("The document '" + reference + "' was bigger (" + contentResource.getContentLength() + "B) " +
                        "than the maximum expected size (" + documentMaximumSize + "B), its content won't be handled");
                return new ByteArrayInputStream(EMPTY_DOCUMENT);
            } else {
                return contentResource.streamContent();
            }
        } catch (Exception ex) {
            throw new RuntimeException("Failed to obtain content from " + reference, ex);
        }
    }

    @Override
    public String getContentType(String reference) {
        try {
            return contentHostingService.getResource(getId(reference)).getContentType();
        } catch (Exception e) {
            logger.info("Couldn't get the contentType of '" + reference + "'");
            return null;
        }
    }

    @Override
    public String getResourceName(String reference) {
        try {
            return contentHostingService.getResource(getId(reference)).getReference();
        } catch (Exception e) {
            logger.info("Couldn't get the contentType of '" + reference + "'");
            return null;
        }
    }

    public void setSupportedResourceTypes(List<String> supportedResourceTypes) {
        this.supportedResourceTypes = supportedResourceTypes;
    }

    public void setDocumentMaximumSize(long documentMaximumSize) {
        this.documentMaximumSize = documentMaximumSize;
    }
}
