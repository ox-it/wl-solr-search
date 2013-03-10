package org.sakaiproject.search.solr.indexing;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.util.AbstractSolrTestCase;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sakaiproject.search.api.EntityContentProducer;
import org.sakaiproject.search.api.SearchService;
import org.sakaiproject.search.producer.ContentProducerFactory;
import org.sakaiproject.search.producer.ProducersHelper;

import java.util.Date;

import static org.hamcrest.CoreMatchers.is;

/**
 * @author Colin Hebert
 */
public class SolrTaskHandlerIT extends AbstractSolrTestCase {
    private SolrTools solrTools;
    private ContentProducerFactory contentProducerFactory;
    private SolrServer solrServer;
    private SolrTaskHandler solrTaskHandler;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initCore("org/sakaiproject/search/solr/conf/search/conf/solrconfig.xml",
                "org/sakaiproject/search/solr/conf/search/conf/schema.xml",
                "org/sakaiproject/search/solr/conf/search/");
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        solrServer = new EmbeddedSolrServer(h.getCoreContainer(), h.getCore().getName());
        solrServer.deleteByQuery("*:*");

        solrTaskHandler = new SolrTaskHandler();
        solrTaskHandler.setSolrServer(solrServer);
        solrTools = new SolrTools();
        solrTaskHandler.setSolrTools(solrTools);
        solrTools.setSolrServer(solrServer);
        contentProducerFactory = new ContentProducerFactory();
        solrTools.setContentProducerFactory(contentProducerFactory);
    }

    @Test
    public void testIndexDocument() throws Exception {
        String reference = "testIndexDocument";
        Date actionDate = new Date();
        EntityContentProducer entityContentProducer = ProducersHelper.getStringContentProducer(reference);
        contentProducerFactory.addContentProducer(entityContentProducer);
        assertIndexIsEmpty();

        solrTaskHandler.indexDocument(reference, actionDate);

        SolrDocumentList result = getSolrDocuments();
        assertThat(result.getNumFound(), is(1L));
        SolrDocument document = result.get(0);
        assertDocumentMatches(document, reference);
    }

    private void assertIndexIsEmpty() throws Exception {
        SolrDocumentList result = getSolrDocuments();
        assertThat(result.getNumFound(), is(0L));
    }

    private void assertDocumentMatches(SolrDocument document, String reference) {
        EntityContentProducer contentProducer = contentProducerFactory.getContentProducerForElement(reference);

        assertThat(document.getFieldValue(SearchService.FIELD_REFERENCE),
                CoreMatchers.<Object>equalTo(reference));
        assertThat(document.getFieldValue(SearchService.FIELD_CONTAINER),
                CoreMatchers.<Object>equalTo(contentProducer.getContainer(reference)));
        assertThat(document.getFieldValue(SearchService.FIELD_TYPE),
                CoreMatchers.<Object>equalTo(contentProducer.getType(reference)));
        assertThat(document.getFieldValue(SearchService.FIELD_TITLE),
                CoreMatchers.<Object>equalTo(contentProducer.getTitle(reference)));
        assertThat(document.getFieldValue(SearchService.FIELD_TOOL),
                CoreMatchers.<Object>equalTo(contentProducer.getTool()));
        assertThat(document.getFieldValue(SearchService.FIELD_URL),
                CoreMatchers.<Object>equalTo(contentProducer.getUrl(reference)));
        assertThat(document.getFieldValue(SearchService.FIELD_SITEID),
                CoreMatchers.<Object>equalTo(contentProducer.getSiteId(reference)));
    }

    private SolrDocumentList getSolrDocuments() throws Exception {
        solrServer.commit();
        SolrQuery query = new SolrQuery("*:*");
        QueryResponse qr = solrServer.query(query);
        return qr.getResults();
    }
}
