package uk.ac.ox.oucs.search.solr.filter;

import org.sakaiproject.search.api.SearchResult;

/**
 * @author Colin Hebert
 */
public class CompatibilitySearchFilter implements SearchItemFilter{
    private final org.sakaiproject.search.filter.SearchItemFilter searchItemFilter;

    public CompatibilitySearchFilter(org.sakaiproject.search.filter.SearchItemFilter searchItemFilter) {
        this.searchItemFilter = searchItemFilter;
    }

    @Override
    public SearchResult filter(SearchResult result) {
        return searchItemFilter.filter(result);
    }
}
