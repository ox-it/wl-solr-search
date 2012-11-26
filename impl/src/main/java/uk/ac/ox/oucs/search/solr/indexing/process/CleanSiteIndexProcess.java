package uk.ac.ox.oucs.search.solr.indexing.process;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.sakaiproject.search.api.SearchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ox.oucs.search.indexing.exception.ProcessExecutionException;
import uk.ac.ox.oucs.search.indexing.exception.TemporaryProcessExecutionException;

import java.io.IOException;

/**
 * @author Colin Hebert
 */
public class CleanSiteIndexProcess implements SolrProcess {
    private static final Logger logger = LoggerFactory.getLogger(CleanSiteIndexProcess.class);
    private final SolrServer solrServer;
    private final String siteId;

    public CleanSiteIndexProcess(SolrServer solrServer, String siteId) {
        this.solrServer = solrServer;
        this.siteId = siteId;
    }

    @Override
    public void execute() {
        logger.info("Removing content for site '" + siteId + "'");
        try {
            solrServer.deleteByQuery(SearchService.FIELD_SITEID + ":" + ClientUtils.escapeQueryChars(siteId));
        } catch (IOException e) {
            throw new TemporaryProcessExecutionException("An exception occurred while cleaning the index of '" + siteId + "'", e);
        } catch (Exception e) {
            throw new ProcessExecutionException("An exception occurred while cleaning the index of '" + siteId + "'", e);
        }
    }
}
