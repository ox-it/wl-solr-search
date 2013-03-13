package org.sakaiproject.search.solr.indexing;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.util.AbstractSolrTestCase;
import org.hamcrest.CoreMatchers;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sakaiproject.search.api.EntityContentProducer;
import org.sakaiproject.search.api.SearchService;
import org.sakaiproject.search.producer.ContentProducerFactory;
import org.sakaiproject.search.producer.ProducerBuilder;

import java.util.Date;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

/**
 * Integration test with Solr.
 *
 * @author Colin Hebert
 */
@org.apache.lucene.util.LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40"})
public class SolrTaskHandlerIT extends AbstractSolrTestCase {
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
        clearIndex();
        assertIndexIsEmpty();

        solrTaskHandler = new SolrTaskHandler();
        solrTaskHandler.setSolrServer(solrServer);
        SolrTools solrTools = new SolrTools();
        solrTaskHandler.setSolrTools(solrTools);
        solrTools.setSolrServer(solrServer);
        contentProducerFactory = new ContentProducerFactory();
        solrTools.setContentProducerFactory(contentProducerFactory);
    }

    /**
     * Attempts to add a new document to the index.
     * <p>
     * Checks that only one document is available (the index is empty to begin with).<br />
     * Checks that the document is the one created (same properties, same creation date).
     * </p>
     *
     * @throws Exception any exception.
     */
    @Test
    public void testIndexDocument() throws Exception {
        String reference = "testIndexDocument";
        DateTime actionDate = new DateTime(2013, 3, 10, 17, 0, 0);
        // Add a producer for 'reference'
        contentProducerFactory.addContentProducer(ProducerBuilder.create().addDoc(reference).build());

        solrTaskHandler.indexDocument(reference, actionDate.toDate());

        SolrDocumentList results = getSolrDocuments();
        // A new documents has been created
        assertThat(results.getNumFound(), is(1L));
        // The document matches the input
        SolrDocument document = results.get(0);
        assertThat(document.getFieldValue(SearchService.FIELD_REFERENCE), CoreMatchers.<Object>equalTo(reference));
        assertDocumentMatches(document, actionDate.toDate());
    }

    /**
     * Attempts to add an outdated document to the index.
     * <p>
     * Checks that a same document can't be twice in the index.<br />
     * Checks that an outdated document doesn't overwrite a newer version of that document.<br />
     * Checks that outdated documents fail silently.
     * </p>
     *
     * @throws Exception any exception.
     */
    @Test
    public void testIndexDocumentOutdatedFails() throws Exception {
        String reference = "testIndexDocument";
        DateTime firstIndexationDate = new DateTime(2013, 3, 10, 18, 0, 0);
        // The secondIndexation date is _before_ the first indexation.
        DateTime secondIndexationDate = new DateTime(2013, 3, 10, 17, 0, 0);
        ProducerBuilder contentProducerBuilder = ProducerBuilder.create().addDoc(reference);
        contentProducerFactory.addContentProducer(contentProducerBuilder.build());
        addDocumentToIndex(reference, firstIndexationDate);

        solrTaskHandler.indexDocument(reference, secondIndexationDate.toDate());

        SolrDocumentList results = getSolrDocuments();
        assertThat(results.getNumFound(), is(1L));

        SolrDocument document = results.get(0);
        assertThat(document.getFieldValue(SearchService.FIELD_REFERENCE), CoreMatchers.<Object>equalTo(reference));
        assertDocumentMatches(document, firstIndexationDate.toDate());
    }

    /**
     * Attempts to remove a document from the index.
     * <p>
     * Checks that the removal of the only document results in an empty index.
     * </p>
     *
     * @throws Exception any exception.
     */
    @Test
    public void testRemoveDocument() throws Exception {
        String reference = "testRemoveDocument";
        DateTime indexationDate = new DateTime(2013, 3, 10, 16, 0, 0);
        DateTime removalDate = new DateTime(2013, 3, 10, 17, 0, 0);
        ProducerBuilder contentProducerBuilder = ProducerBuilder.create().addDoc(reference);
        contentProducerFactory.addContentProducer(contentProducerBuilder.build());
        addDocumentToIndex(reference, indexationDate);

        solrTaskHandler.removeDocument(reference, removalDate.toDate());

        assertIndexIsEmpty();
    }

    /**
     * Attempts to remove a document from the index when a newer version of the document is present.
     * <p>
     * Checks that a newer document isn't removed when the removal date is before the document indexation date.<br />
     * Checks that outdated removal fail silently.
     * </p>
     *
     * @throws Exception any exception.
     */
    @Test
    public void testRemoveDocumentOutdatedFails() throws Exception {
        String reference = "testRemoveDocument";
        DateTime indexationDate = new DateTime(2013, 3, 10, 18, 0, 0);
        // The removal date is _before_ the indexation.
        DateTime removalDate = new DateTime(2013, 3, 10, 17, 0, 0);
        ProducerBuilder contentProducerBuilder = ProducerBuilder.create().addDoc(reference);
        contentProducerFactory.addContentProducer(contentProducerBuilder.build());
        addDocumentToIndex(reference, indexationDate);

        solrTaskHandler.removeDocument(reference, removalDate.toDate());

        assertThat(getSolrDocuments().getNumFound(), is(1L));
    }

    /**
     * Attempts to index a site containing multiple documents.
     * <p>
     * Checks that the expected number of documents were indexed.
     * </p>
     *
     * @throws Exception any exception.
     */
    @Test
    public void testIndexSiteCreatesRightNumberOfDocuments() throws Exception {
        String siteId = "indexSiteId";
        int numberOfDocs = 7;
        DateTime actionDate = new DateTime(2013, 3, 10, 17, 0, 0);
        ProducerBuilder contentProducerBuilder = ProducerBuilder.create()
                .addDocToSite(siteId, numberOfDocs);
        contentProducerFactory.addContentProducer(contentProducerBuilder.build());

        solrTaskHandler.indexSite(siteId, actionDate.toDate());

        assertThat(getSolrDocuments().getNumFound(), is((long) numberOfDocs));
        assertSiteDocumentsMatches(siteId, actionDate.toDate());
    }

    /**
     * Attempt to index an already indexed site containing multiple documents (and had some documents removed).
     * <p>
     * Checks that the expected number of documents were indexed.<br />
     * Checks that the old/nonexistant documents were removed.<br />
     * </p>
     *
     * @throws Exception any exception.
     */
    @Test
    public void testIndexSiteRemovesOldDocuments() throws Exception {
        String siteId = "indexSiteId";
        int numberOfOldDocs = 7;
        DateTime indexationDate = new DateTime(2013, 3, 10, 17, 0, 0);
        int numberOfNewDocs = 3;
        DateTime actionDate = new DateTime(2013, 3, 10, 18, 0, 0);
        ProducerBuilder contentProducerBuilder = ProducerBuilder.create().addDocToSite(siteId, numberOfOldDocs);
        contentProducerFactory.addContentProducer(contentProducerBuilder.build());
        addSiteToIndex(siteId, indexationDate);
        contentProducerBuilder.emptySite(siteId)
                .addDocToSite(siteId, numberOfNewDocs);

        solrTaskHandler.indexSite(siteId, actionDate.toDate());

        assertThat(getSolrDocuments().getNumFound(), is((long) numberOfNewDocs));
        assertSiteDocumentsMatches(siteId, actionDate.toDate());
    }

    private void assertIndexIsEmpty() throws Exception {
        assertThat(getSolrDocuments().getNumFound(), is(0L));
    }

    private void assertSiteDocumentsMatches(String siteId, Date actionDate) throws Exception {
        QueryResponse response = solrServer.query(
                new SolrQuery(SearchService.FIELD_SITEID + ":" + ClientUtils.escapeQueryChars(siteId)));

        for (SolrDocument document : response.getResults()) {
            assertDocumentMatches(document, actionDate);
        }
    }


    private void assertDocumentMatches(SolrDocument document, Date actionDate) {
        assertDocumentMatches(document);
        assertThat((Date) document.getFieldValue(SearchService.DATE_STAMP), equalTo(actionDate));
    }

    private void assertDocumentMatches(SolrDocument document) {
        String reference = (String) document.getFieldValue(SearchService.FIELD_REFERENCE);
        EntityContentProducer contentProducer = contentProducerFactory.getContentProducerForElement(reference);

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

    private void addDocumentToIndex(String reference, DateTime indexationDate) throws Exception {
        solrTaskHandler.indexDocument(reference, indexationDate.toDate());
        solrServer.commit();
    }

    private void addSiteToIndex(String siteId, DateTime indexationDate) throws Exception {
        solrTaskHandler.indexSite(siteId, indexationDate.toDate());
        solrServer.commit();
    }

    private SolrDocumentList getSolrDocuments() throws Exception {
        solrServer.commit();
        SolrQuery query = new SolrQuery("*:*");
        QueryResponse qr = solrServer.query(query);
        return qr.getResults();
    }
}
