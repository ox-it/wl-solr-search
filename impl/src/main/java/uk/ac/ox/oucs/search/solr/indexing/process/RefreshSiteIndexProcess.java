package uk.ac.ox.oucs.search.solr.indexing.process;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.sakaiproject.search.api.EntityContentProducer;
import org.sakaiproject.search.api.SearchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ox.oucs.search.producer.ContentProducerFactory;
import uk.ac.ox.oucs.search.indexing.ProcessExecutionException;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Colin Hebert
 */
public class RefreshSiteIndexProcess implements SolrProcess {
    private static final Logger logger = LoggerFactory.getLogger(RefreshSiteIndexProcess.class);
    private final SolrServer solrServer;
    private final ContentProducerFactory contentProducerFactory;
    private final String siteId;

    public RefreshSiteIndexProcess(SolrServer solrServer, ContentProducerFactory contentProducerFactory, String siteId) {
        this.solrServer = solrServer;
        this.contentProducerFactory = contentProducerFactory;
        this.siteId = siteId;
    }

    @Override
    public void execute() {
        logger.info("Refreshing the index for '" + siteId + "'");
        //Get the currently indexed resources for this site
        Collection<String> resourceNames = getResourceNames(siteId);
        logger.debug(resourceNames.size() + " elements will be refreshed");
        new CleanSiteIndexProcess(solrServer, siteId).execute();
        for (String resourceName : resourceNames) {
            EntityContentProducer entityContentProducer = contentProducerFactory.getContentProducerForElement(resourceName);

            //If there is no matching entity content producer or no associated site, skip the resource
            //it is either not available anymore, or the corresponding entityContentProducer doesn't exist anymore
            if (entityContentProducer == null || entityContentProducer.getSiteId(resourceName) == null) {
                logger.warn("Couldn't either find an entityContentProducer or the resource itself for '" + resourceName + "'");
                continue;
            }

            new IndexDocumentProcess(solrServer, entityContentProducer, resourceName).execute();
        }
    }

    /**
     * Get all indexed resources for a site
     *
     * @param siteId Site containing indexed resources
     * @return a collection of resource references or an empty collection if no resource was found
     */
    private Collection<String> getResourceNames(String siteId) {
        try {
            logger.debug("Obtaining indexed elements for site: '" + siteId + "'");
            SolrQuery query = new SolrQuery()
                    .setQuery(SearchService.FIELD_SITEID + ":" + ClientUtils.escapeQueryChars(siteId))
                    .addField(SearchService.FIELD_REFERENCE);
            SolrDocumentList results = solrServer.query(query).getResults();
            Collection<String> resourceNames = new ArrayList<String>(results.size());
            for (SolrDocument document : results) {
                resourceNames.add((String) document.getFieldValue(SearchService.FIELD_REFERENCE));
            }
            return resourceNames;
        } catch (SolrServerException e) {
            throw new ProcessExecutionException("Couldn't get indexed elements for site: '" + siteId + "'", e);
        }
    }
}
