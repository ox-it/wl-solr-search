package uk.ac.ox.oucs.search.solr.indexing.process;

import org.apache.solr.client.solrj.SolrServer;
import org.sakaiproject.search.api.EntityContentProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ox.oucs.search.solr.indexing.exception.ProcessExecutionException;
import uk.ac.ox.oucs.search.solr.indexing.exception.TemporaryProcessExecutionException;

import java.io.IOException;

/**
 * @author Colin Hebert
 */
public class RemoveDocumentProcess implements SolrProcess {
    private static final Logger logger = LoggerFactory.getLogger(RemoveDocumentProcess.class);
    private final SolrServer solrServer;
    private final EntityContentProducer entityContentProducer;
    private final String resourceName;

    public RemoveDocumentProcess(SolrServer solrServer, EntityContentProducer entityContentProducer, String resourceName) {
        this.solrServer = solrServer;
        this.entityContentProducer = entityContentProducer;
        this.resourceName = resourceName;
    }

    @Override
    public void execute() {
        logger.debug("Remove '" + resourceName + "' from the index");
        try {
            solrServer.deleteById(entityContentProducer.getId(resourceName));
        } catch (IOException e) {
            throw new TemporaryProcessExecutionException("An exception occurred while removing the document '" + resourceName + "'", e);
        } catch (Exception e) {
            throw new ProcessExecutionException("An exception occurred while removing the document '" + resourceName + "'", e);
        }
    }
}
