package uk.ac.ox.oucs.search.solr.response;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.solr.common.SolrDocument;
import org.sakaiproject.search.api.SearchResult;
import org.sakaiproject.search.api.SearchService;
import org.sakaiproject.search.api.TermFrequency;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

import static uk.ac.ox.oucs.search.solr.response.QueryResponseTermVectorExtractor.TermInfo;

/**
 * @author Colin Hebert
 */
public class SolrResult implements SearchResult {
    public static final String SCORE_FIELD = "score";
    private final int index;
    private final SolrDocument document;
    private final Map<String, List<String>> highlights;
    private final TermFrequency terms;
    private String newUrl;

    public SolrResult(int index, SolrDocument document, Map<String, List<String>> highlights, Map<String, Map<String, TermInfo>> terms) {
        this.index = index;
        this.document = document;
        this.highlights = highlights;
        this.terms = extractTermFrequency(terms);
    }

    @Override
    public float getScore() {
        return (Float) document.getFieldValue(SCORE_FIELD);
    }

    @Override
    public String getId() {
        return (String) document.getFieldValue(SearchService.FIELD_ID);
    }

    @Override
    public String[] getFieldNames() {
        Collection<String> fieldNames = document.getFieldNames();
        return fieldNames.toArray(new String[fieldNames.size()]);
    }

    @Override
    public String[] getValues(String fieldName) {
        return collectionToStringArray(document.getFieldValues(fieldName));
    }

    @Override
    public Map<String, String[]> getValueMap() {
        Map<String, Collection<Object>> valueMapObject = document.getFieldValuesMap();
        Map<String, String[]> valueMap = new HashMap<String, String[]>(valueMapObject.size(), 1);
        for (Map.Entry<String, Collection<Object>> entry : valueMapObject.entrySet()) {
            valueMap.put(entry.getKey(), collectionToStringArray(entry.getValue()));
        }
        return valueMap;
    }

    @Override
    public String getUrl() {
        return (newUrl == null) ? (String) document.getFieldValue(SearchService.FIELD_URL) : newUrl;
    }

    @Override
    public String getTitle() {
        return (String) document.getFieldValue(SearchService.FIELD_TITLE);
    }

    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public String getSearchResult() {
        StringBuilder sb = new StringBuilder();
        for(Map.Entry<String, List<String>> fieldEntry : highlights.entrySet()){
            sb.append(fieldEntry.getKey()).append(": ");
            for(String highlight : fieldEntry.getValue())
                sb.append(highlight).append("... ");
        }
        return sb.toString();
    }

    @Override
    public String getReference() {
        return (String) document.getFieldValue(SearchService.FIELD_REFERENCE);
    }

    @Override
    public TermFrequency getTerms() throws IOException {
        return terms;
    }

    @Override
    public String getTool() {
        return (String) document.getFieldValue(SearchService.FIELD_TOOL);
    }

    @Override
    public boolean isCensored() {
        return false;
    }

    @Override
    public String getSiteId() {
        return (String) document.getFieldValue(SearchService.FIELD_SITEID);
    }

    @Override
    public void toXMLString(StringBuilder sb) {
        sb.append("<result");
        sb.append(" index=\"").append(getIndex()).append("\" ");
        sb.append(" score=\"").append(getScore()).append("\" ");
        sb.append(" sid=\"").append(StringEscapeUtils.escapeXml(getId())).append("\" ");
        sb.append(" site=\"").append(StringEscapeUtils.escapeXml(getSiteId())).append("\" ");
        sb.append(" reference=\"").append(StringEscapeUtils.escapeXml(getReference())).append("\" ");
        try {
            sb.append(" title=\"").append(new String(Base64.encodeBase64(getTitle().getBytes("UTF-8")), "UTF-8")).append("\" ");
        } catch (UnsupportedEncodingException e) {
            sb.append(" title=\"").append(StringEscapeUtils.escapeXml(getTitle())).append("\" ");
        }
        sb.append(" tool=\"").append(StringEscapeUtils.escapeXml(getTool())).append("\" ");
        sb.append(" url=\"").append(StringEscapeUtils.escapeXml(getUrl())).append("\" />");
    }

    @Override
    public void setUrl(String newUrl) {
        this.newUrl = newUrl;
    }

    @Override
    public boolean hasPortalUrl() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    private static String[] collectionToStringArray(Collection<?> objectValues) {
        String[] values = new String[objectValues.size()];
        int i = 0;
        for (Iterator<?> iterator = objectValues.iterator(); iterator.hasNext(); i++) {
            values[i] = iterator.next().toString();
        }
        return values;
    }

    /**
     * Extract a {@link TermFrequency} from the result of a {@link org.apache.solr.handler.component.TermVectorComponent}
     *
     * @param termsByField A map of field/terms
     * @return
     */
    private static TermFrequency extractTermFrequency(Map<String, Map<String, TermInfo>> termsByField) {
        Map<String, Long> termFrequencies = new HashMap<String, Long>();
        //Count the frequencies for each term, based on the sum of the frequency in each field
        for (Map<String, TermInfo> terms : termsByField.values()) {
            for (Map.Entry<String, TermInfo> term : terms.entrySet()) {
                Long addedFrequency = term.getValue().getTermFrequency();
                //Ignore when the frequency isn't specified (if tf isn't returned by solr)
                if (addedFrequency == null)
                    continue;
                Long frequency = termFrequencies.get(term.getKey());
                termFrequencies.put(term.getKey(), (frequency == null) ? addedFrequency : addedFrequency + frequency);
            }
        }

        //Sort tuples (Term/Frequency)
        //A SortedSet consider that two elements that are equals based on compare are the same
        //This is why, if the frequency is the same, then the term is used to do the comparison
        SortedSet<Map.Entry<String, Long>> sortedFrequencies = new TreeSet<Map.Entry<String, Long>>(new Comparator<Map.Entry<String, Long>>() {
            @Override
            public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                int longComparison = o1.getValue().compareTo(o2.getValue());
                return (longComparison != 0) ? longComparison : o1.getKey().compareTo(o2.getKey());
            }
        });
        sortedFrequencies.addAll(termFrequencies.entrySet());

        //Extract data from each Entry into two arrays
        final String[] terms = new String[sortedFrequencies.size()];
        final int[] frequencies = new int[sortedFrequencies.size()];
        int i = 0;
        for (Map.Entry<String, Long> term : sortedFrequencies) {
            terms[i] = term.getKey();
            //There is a huge loss in precision, but there should not be any issue with null values
            frequencies[i] = (int) (long) term.getValue();
            i++;
        }

        return new TermFrequency() {
            @Override
            public String[] getTerms() {
                return terms;
            }

            @Override
            public int[] getFrequencies() {
                return frequencies;
            }
        };
    }
}