package uk.ac.ox.oucs.search.solr.indexing.process;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.sakaiproject.search.api.SearchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ox.oucs.search.ContentProducerFactory;
import uk.ac.ox.oucs.search.indexing.ProcessExecutionException;
import uk.ac.ox.oucs.search.indexing.TemporaryProcessExecutionException;

import java.io.IOException;
import java.util.Queue;

/**
 * @author Colin Hebert
 */
public class RebuildIndexProcess implements SolrProcess {
    private static final Logger logger = LoggerFactory.getLogger(RebuildIndexProcess.class);
    private final SolrServer solrServer;
    private final Queue<String> reindexedSites;
    private final ContentProducerFactory contentProducerFactory;

    public RebuildIndexProcess(SolrServer solrServer, Queue<String> reindexedSites, ContentProducerFactory contentProducerFactory) {
        this.solrServer = solrServer;
        this.reindexedSites = reindexedSites;
        this.contentProducerFactory = contentProducerFactory;
    }

    @Override
    public void execute() {
        logger.info("Rebuilding the index for every indexable site");
        StringBuilder sb = new StringBuilder();
        while (!reindexedSites.isEmpty()) {
            String siteId = reindexedSites.poll();
            new BuildSiteIndexProcess(solrServer, contentProducerFactory, siteId).execute();
            sb.append(" -").append(ClientUtils.escapeQueryChars(siteId));
        }
        logger.info("Remove indexed documents for unindexable or non-existing sites");
        try {
            solrServer.deleteByQuery(SearchService.FIELD_SITEID + ":( " + sb + " )");
        } catch (IOException e) {
            throw new TemporaryProcessExecutionException("An exception occurred while removing obsoletes sites from the index", e);
        } catch (Exception e) {
            throw new ProcessExecutionException("An exception occurred while removing obsoletes sites from the index", e);
        }
    }
}
