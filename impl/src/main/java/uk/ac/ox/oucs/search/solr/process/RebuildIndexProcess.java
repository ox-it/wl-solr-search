package uk.ac.ox.oucs.search.solr.process;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.sakaiproject.search.api.SearchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ox.oucs.search.solr.ContentProducerFactory;

import java.util.Collection;

/**
 * @author Colin Hebert
 */
public class RebuildIndexProcess implements SolrProcess {
    private static final Logger logger = LoggerFactory.getLogger(RebuildIndexProcess.class);
    private final SolrServer solrServer;
    private final Collection<String> reindexedSites;
    private final ContentProducerFactory contentProducerFactory;

    public RebuildIndexProcess(SolrServer solrServer, Collection<String> reindexedSites, ContentProducerFactory contentProducerFactory) {
        this.solrServer = solrServer;
        this.reindexedSites = reindexedSites;
        this.contentProducerFactory = contentProducerFactory;
    }

    @Override
    public void execute() {
        logger.info("Rebuilding the index for every indexable site");
        StringBuilder sb = new StringBuilder();
        for (String siteId : reindexedSites) {
            new BuildSiteIndexProcess(solrServer, contentProducerFactory, siteId).execute();
            sb.append(" -").append(ClientUtils.escapeQueryChars(siteId));
        }
        logger.info("Remove indexed documents for unindexable or non-existing sites");
        try {
            solrServer.deleteByQuery(SearchService.FIELD_SITEID + ":( " + sb + " )");
        } catch (Exception e) {
            logger.error("An exception occurred while removing obsoletes sites from the index", e);
        }
    }
}
