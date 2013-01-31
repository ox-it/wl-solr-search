package org.sakaiproject.search.response.filter;

import org.sakaiproject.search.api.EntityContentProducer;
import org.sakaiproject.search.api.SearchResult;
import org.sakaiproject.search.api.TermFrequency;
import org.sakaiproject.search.response.filter.SearchItemFilter;
import uk.ac.ox.oucs.search.producer.ContentProducerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class SecuritySearchFilter implements SearchItemFilter {
    public static final SearchResult censoredSearchResult = new CensoredSearchResult();
    private ContentProducerFactory contentProducerFactory;

    @Override
    public SearchResult filter(SearchResult result) {
        String reference = result.getReference();
        EntityContentProducer contentProducer = contentProducerFactory.getContentProducerForElement(reference);
        return contentProducer == null || !contentProducer.canRead(reference) ? censoredSearchResult : result;
    }

    public void setContentProducerFactory(ContentProducerFactory contentProducerFactory) {
        this.contentProducerFactory = contentProducerFactory;
    }

    private static class CensoredSearchResult implements SearchResult {
        private static final TermFrequency termFrequency = new TermFrequency() {
            @Override
            public String[] getTerms() {
                return new String[0];
            }

            @Override
            public int[] getFrequencies() {
                return new int[0];
            }
        };

        @Override
        public float getScore() {
            return 0;
        }

        @Override
        public String getId() {
            return "";
        }

        @Override
        public String[] getFieldNames() {
            return new String[0];
        }

        @Override
        public String[] getValues(String string) {
            return new String[0];
        }

        @Override
        public Map<String, String[]> getValueMap() {
            return Collections.emptyMap();
        }

        @Override
        public String getUrl() {
            return "";
        }

        @Override
        public void setUrl(String newUrl) {
        }

        @Override
        public String getTitle() {
            return "";
        }

        @Override
        public int getIndex() {
            return 0;
        }

        @Override
        public String getSearchResult() {
            return "";
        }

        @Override
        public String getReference() {
            return "";
        }

        @Override
        public TermFrequency getTerms() throws IOException {
            return termFrequency;
        }

        @Override
        public String getTool() {
            return "";
        }

        @Override
        public boolean isCensored() {
            return true;
        }

        @Override
        public String getSiteId() {
            return "";
        }

        @Override
        public void toXMLString(StringBuilder sb) {
        }

        @Override
        public boolean hasPortalUrl() {
            return false;
        }
    }
}
