package uk.ac.ox.oucs.search.solr.filter;

import org.sakaiproject.search.api.SearchResult;

public interface SearchItemFilter {
    SearchResult filter(SearchResult result);
}
