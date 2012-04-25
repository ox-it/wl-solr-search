package uk.ac.ox.oucs.search.solr.response;

import com.google.common.collect.ForwardingList;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.util.NamedList;
import org.sakaiproject.search.api.SearchList;
import org.sakaiproject.search.api.SearchResult;
import org.sakaiproject.search.api.SearchService;

import java.util.*;

import static uk.ac.ox.oucs.search.solr.response.QueryResponseTermVectorExtractor.TermInfo;

/**
 * @author Colin Hebert
 */
public class SolrSearchList extends ForwardingList<SearchResult> implements SearchList {
    private final List<SearchResult> solrResults;
    private final QueryResponse rsp;
    private final int start;

    public SolrSearchList(QueryResponse rsp) {
        this.rsp = rsp;

        //Get the 'start' value. If not set, use 0
        String expectedStart = ((NamedList<String>) rsp.getHeader().get("params")).get("start");
        this.start = (expectedStart != null) ? Integer.parseInt(expectedStart) : 0;

        solrResults = new ArrayList<SearchResult>(rsp.getResults().size());

        //Extract TermVector informations from the response
        QueryResponseTermVectorExtractor qrtve = new QueryResponseTermVectorExtractor(rsp);
        Map<String, Map<String, Map<String, TermInfo>>> termsPerDocument = qrtve.getTermVectorInfo();

        for (SolrDocument document : rsp.getResults()) {
            String id = (String) document.getFieldValue(SearchService.FIELD_ID);
            Map<String, List<String>> highlight = rsp.getHighlighting().get(id);
            Map<String, Map<String, TermInfo>> terms = termsPerDocument.get(id);
            solrResults.add(new SolrResult(solrResults.size(), document, highlight, terms));
        }
    }


    @Override
    public Iterator<SearchResult> iterator(int startAt) {
        Iterator<SearchResult> iterator = iterator();
        //Skip the fist elements
        for (int i = 0; i < startAt && iterator.hasNext(); i++)
            iterator.next();
        return iterator;
    }

    @Override
    public int getFullSize() {
        return (int) rsp.getResults().getNumFound();
    }

    @Override
    public int getStart() {
        return start;
    }

    @Override
    protected List<SearchResult> delegate() {
        return Collections.unmodifiableList(solrResults);
    }
}
