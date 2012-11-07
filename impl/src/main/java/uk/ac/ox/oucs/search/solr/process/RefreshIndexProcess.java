package uk.ac.ox.oucs.search.solr.process;

import org.apache.solr.client.solrj.SolrServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ox.oucs.search.solr.ContentProducerFactory;

import java.util.Collection;

/**
 * @author Colin Hebert
 */
public class RefreshIndexProcess implements SolrProcess {
    private static final Logger logger = LoggerFactory.getLogger(RefreshIndexProcess.class);
    private final SolrServer solrServer;
    private final Collection<String> refreshedSites;
    private final ContentProducerFactory contentProducerFactory;

    public RefreshIndexProcess(SolrServer solrServer, Collection<String> refreshedSites, ContentProducerFactory contentProducerFactory) {
        this.solrServer = solrServer;
        this.refreshedSites = refreshedSites;
        this.contentProducerFactory = contentProducerFactory;
    }

    @Override
    public void execute() {
        logger.info("Refreshing the index for every indexable site");
        for (String siteId : refreshedSites) {
            new RefreshSiteIndexProcess(solrServer, contentProducerFactory, siteId).execute();
        }
    }
}