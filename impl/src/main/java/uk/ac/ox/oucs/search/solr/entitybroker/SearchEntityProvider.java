package uk.ac.ox.oucs.search.solr.entitybroker;

import org.sakaiproject.entitybroker.EntityReference;
import org.sakaiproject.entitybroker.EntityView;
import org.sakaiproject.entitybroker.entityprovider.annotations.EntityCustomAction;
import org.sakaiproject.entitybroker.entityprovider.capabilities.ActionsExecutable;
import org.sakaiproject.entitybroker.entityprovider.capabilities.Describeable;
import org.sakaiproject.entitybroker.entityprovider.capabilities.Outputable;
import org.sakaiproject.entitybroker.entityprovider.extension.Formats;
import org.sakaiproject.entitybroker.entityprovider.search.Restriction;
import org.sakaiproject.entitybroker.entityprovider.search.Search;
import org.sakaiproject.entitybroker.util.AbstractEntityProvider;
import org.sakaiproject.search.api.InvalidSearchQueryException;
import org.sakaiproject.search.api.SearchList;
import org.sakaiproject.search.api.SearchResult;
import org.sakaiproject.search.api.SearchService;
import org.sakaiproject.site.api.Site;
import org.sakaiproject.site.api.SiteService;
import org.sakaiproject.user.api.UserDirectoryService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Entity provider for Entity broker giving access to search services through a an HTTP method
 *
 * @author Adrian Fish (a.fish@lancaster.ac.uk)
 * @author Colin Hebert
 */
public class SearchEntityProvider extends AbstractEntityProvider implements ActionsExecutable, Outputable, Describeable {
    private SearchService searchService;
    private SiteService siteService;
    public UserDirectoryService userDirectoryService;

    /**
     * Name of the service, here "search"
     *
     * @return the constant name of this service
     */
    @Override
    public String getEntityPrefix() {
        return "search";
    }

    /**
     * Handled formats, such as JSon and XML
     *
     * @return formats supported
     */
    @Override
    public String[] getHandledOutputFormats() {
        return new String[]{Formats.JSON, Formats.XML};
    }

    /**
     * Simple search method
     *
     * @param ref
     * @param search
     * @return a list of SearchResults
     */
    @EntityCustomAction(action = "search", viewKey = EntityView.VIEW_LIST)
    public List<SearchResultEntity> search(EntityReference ref, Search search) {
        try {
            //Get the query sent by the client
            String query = extractQuery(search.getRestrictionByProperty("searchTerms"));
            //Get the list of contexts (sites) used for this search, or every accessible site if the user hasn't provided a context lsit
            List<String> contexts = extractContexts(search.getRestrictionByProperty("contexts"));

            //Actual search
            SearchList searchResults = searchService.search(query, contexts, (int) search.getStart(), (int) search.getLimit());

            //Transforms SearchResult in a SearchResultEntity to avoid conflicts with the getId() method (see SRCH-85)
            List<SearchResultEntity> results = new ArrayList<SearchResultEntity>(searchResults.size());
            for (SearchResult result : searchResults) {
                results.add(new SearchResultEntity(result));
            }

            return results;
        } catch (InvalidSearchQueryException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Extract the query from users parameters
     *
     * @param searchTermsRestriction parameter given to EntityBroker
     * @return A search String
     * @throws IllegalArgumentException If no query has been provided
     */
    private String extractQuery(Restriction searchTermsRestriction) {
        if (searchTermsRestriction == null)
            throw new IllegalArgumentException("No searchTerms supplied");

        StringBuilder searchQuery = new StringBuilder();
        for (String term : (String[]) searchTermsRestriction.getArrayValue()) {
            //Concatenate with spaces, if the user wants a coma separated value he can easily enter comas in his query.
            searchQuery.append(term).append(' ');
        }
        return searchQuery.toString();
    }

    /**
     * Extract contexts from users parameters
     *
     * @param contextsRestriction parameter given to EntityBroker
     * @return A list of contexts (sites) where the search will be done
     */
    private List<String> extractContexts(Restriction contextsRestriction) {
        List<String> contexts;
        if (contextsRestriction != null)
            contexts = Arrays.asList((String[]) contextsRestriction.getArrayValue());
        else
            // No contexts supplied. Get all the sites the current user is a member of
            contexts = getAllSites();
        return contexts;
    }

    /**
     * Get all sites available for the current user
     *
     * @return a list of contexts (sites IDs) available for the current user
     */
    private List<String> getAllSites() {
        List<Site> sites = siteService.getSites(SiteService.SelectionType.ACCESS, null, null, null, null, null);
        List<String> siteIds = new ArrayList<String>(sites.size());
        for (Site site : sites) {
            if (site != null && site.getId() != null)
                siteIds.add(site.getId());
        }

        //Manually add the user's site
        siteIds.add(siteService.getUserSiteId(userDirectoryService.getCurrentUser().getId()));
        return siteIds;
    }

    /**
     * A wrapper to customise the result sent through EntityBroker
     * <p>
     * Wraps a {@link SearchResult} to avoid issues with the {@link org.sakaiproject.search.api.SearchResult#getId()}
     * method and {@link EntityReference#checkPrefixId(String, String)}.<br />
     * Can also filter which parts of the query are accessible to a remote user.
     * </p>
     */
    public class SearchResultEntity {
        private final SearchResult searchResult;

        private SearchResultEntity(SearchResult searchResult) {
            this.searchResult = searchResult;
        }

        public String getContentId() {
            return searchResult.getId();
        }

        public float getScore() {
            return searchResult.getScore();
        }

        public String getSearchResult() {
            return searchResult.getSearchResult();
        }

        public String getTitle() {
            return searchResult.getTitle();
        }

        public String getTool() {
            return searchResult.getTool();
        }

        public String getUrl() {
            return searchResult.getUrl();
        }
    }

    //--------------------------
    //Spring injected components
    //--------------------------
    public void setSearchService(SearchService searchService) {
        this.searchService = searchService;
    }

    public void setSiteService(SiteService siteService) {
        this.siteService = siteService;
    }

    public void setUserDirectoryService(UserDirectoryService userDirectoryService) {
        this.userDirectoryService = userDirectoryService;
    }
}