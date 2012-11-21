package uk.ac.ox.oucs.search.response.filter;

import org.sakaiproject.search.api.SearchResult;

public interface SearchItemFilter {
    SearchResult filter(SearchResult result);
}
