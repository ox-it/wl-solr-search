package org.sakaiproject.search.producer;

import org.sakaiproject.citation.api.Citation;
import org.sakaiproject.citation.api.CitationCollection;
import org.sakaiproject.citation.api.CitationService;

import java.io.Reader;
import java.util.List;
import java.util.Map;

/**
 * @author Colin Hebert
 */
public class CitationContentProducer extends ContentHostingContentProducer {
    private CitationService citationService;

    @Override
    protected boolean isResourceTypeSupported(String contentType) {
        return CitationService.CITATION_LIST_ID.equals(contentType);
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
        try {
            String citationCollectionId = new String(contentHostingService.getResource(getId(reference)).getContent());
            CitationCollection collection = citationService.getCollection(citationCollectionId);

            StringBuilder sb = new StringBuilder();

            for (Citation citation : (List<Citation>) collection.getCitations()) {
                sb.append(citation.getId()).append('\n')
                        .append(citation.getSchema().getIdentifier()).append('\n');

                for (Map.Entry<String, Object> property : ((Map<String, Object>) citation.getCitationProperties()).entrySet()) {
                    // Some properties are given as Collections (Vector?!) other are simple Strings, it's rather
                    // difficult to check for every possible type of content, so the toString method will be used instead
                    sb.append(property.getKey()).append(':').append(property.getValue()).append('\n');
                }
            }
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setCitationService(CitationService citationService) {
        this.citationService = citationService;
    }

    @Override
    public boolean matches(String reference) {
        return entityManager.newReference(reference).getEntityProducer() instanceof CitationService;
    }
}
