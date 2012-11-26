package uk.ac.ox.oucs.search.solr;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.util.AbstractSolrTestCase;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.sakaiproject.event.api.Event;
import org.sakaiproject.event.api.Notification;
import org.sakaiproject.event.api.NotificationEdit;
import org.sakaiproject.event.api.NotificationService;
import org.sakaiproject.search.api.SearchIndexBuilder;
import org.sakaiproject.search.api.SearchList;
import org.sakaiproject.search.api.SearchResult;
import org.sakaiproject.search.model.SearchBuilderItem;
import org.sakaiproject.site.api.Site;
import org.sakaiproject.site.api.SiteService;
import org.sakaiproject.site.api.ToolConfiguration;
import uk.ac.ox.oucs.search.producer.ContentProducerFactory;
import uk.ac.ox.oucs.search.producer.BinaryEntityContentProducer;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Colin Hebert
 */
public class SolrSearchServiceTest extends AbstractSolrTestCase {
    private EmbeddedSolrServer solrServer;
    private SolrSearchService solrSearchService;
    private final Map<String, String> documents;

    public SolrSearchServiceTest() {
        documents = new HashMap<String, String>();
        documents.put("java", "/uk/ac/ox/oucs/search/solr/Java_(programming_language).html");
        documents.put("lucene", "/uk/ac/ox/oucs/search/solr/Lucene.html");
        documents.put("readme", "/uk/ac/ox/oucs/search/solr/README.markdown");
        documents.put("refcard", "/uk/ac/ox/oucs/search/solr/refcard.pdf");
        documents.put("sakai", "/uk/ac/ox/oucs/search/solr/Sakai_Project.html");
        documents.put("solr", "/uk/ac/ox/oucs/search/solr/Solr.html");
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        NotificationService notificationService = mock(NotificationService.class);
        when(notificationService.addTransientNotification()).thenReturn(mock(NotificationEdit.class));
        solrSearchService = new SolrSearchService();
        solrServer = new EmbeddedSolrServer(h.getCoreContainer(), h.getCore().getName());
        solrSearchService.setSolrServer(solrServer);
    }

    @Test
    public void testSearchOneDocumentContent() throws Exception {
        presetIndex(solrServer);
        SearchList searchList = solrSearchService.search("contents:zsh", null, 0, 10);
        assertEquals(1, searchList.getFullSize());
        SearchResult result = searchList.get(0);
        assertEquals("refcard.id", result.getId());
        assertNotNull(result.getSearchResult());
        assertTrue(result.getTerms().getFrequencies().length > 0);
        assertEquals(result.getTerms().getFrequencies().length, result.getTerms().getTerms().length);
    }

    @Test
    public void testSearchOneDocumentMultipleFields() throws Exception {
        presetIndex(solrServer);
        SearchList searchList = solrSearchService.search("refcard", null, 0, 10);
        assertEquals(1, searchList.getFullSize());
        SearchResult result = searchList.get(0);
        assertEquals("refcard.id", result.getId());
        assertNotNull(result.getSearchResult());
        assertTrue(result.getTerms().getFrequencies().length > 0);
        assertEquals(result.getTerms().getFrequencies().length, result.getTerms().getTerms().length);
    }

    @Test
    public void testSearchMultipleDocumentsContent() throws Exception {
        presetIndex(solrServer);
        SearchList searchList = solrSearchService.search("contents:solr", null, 1, 2);
        assertEquals(2, searchList.getFullSize());
        assertEquals(1, searchList.size());
        SearchResult result = searchList.get(0);
        assertEquals("lucene.id", result.getId());
        assertNotNull(result.getSearchResult());
        assertTrue(result.getTerms().getFrequencies().length > 0);
        assertEquals(result.getTerms().getFrequencies().length, result.getTerms().getTerms().length);
    }

    @Test
    public void testSearchMultipleDocumentsMultipleFields() throws Exception {
        presetIndex(solrServer);
        SearchList searchList = solrSearchService.search("java", null, 1, 3);
        assertEquals(4, searchList.getFullSize());
        assertEquals(2, searchList.size());
        SearchResult result = searchList.get(0);
        assertEquals("lucene.id", result.getId());
        assertNotNull(result.getSearchResult());
        assertTrue(result.getTerms().getFrequencies().length > 0);
        assertEquals(result.getTerms().getFrequencies().length, result.getTerms().getTerms().length);
    }

    @Test
    public void testStatus() throws Exception {
        //Just a ping, 0 is expected when everything is ok, something else otherwise
        assertEquals("0", solrSearchService.getStatus());
        //Works with solr 3.6
        //solrServer.shutdown();
        //assertNotSame(0, solrSearchService.getStatus());
    }

    @Test
    public void testGetNDocs() throws Exception {
        assertEquals(0, solrSearchService.getNDocs());
        presetIndex(solrServer);
        assertEquals(documents.size(), solrSearchService.getNDocs());
    }

    @Test
    public void testGetSearchSuggestion() throws Exception {
        presetIndex(solrServer);
        String suggestion = solrSearchService.getSearchSuggestion("Jav");
        assertTrue("java".equalsIgnoreCase(suggestion));
    }

    @Override
    public String getSchemaFile() {
        return "solr/conf/schema.xml";
    }

    @Override
    public String getSolrConfigFile() {
        return "solr/conf/solrconfig.xml";
    }

    private void presetIndex(SolrServer solrServer) throws Exception {
        SolrSearchIndexBuilder solrSearchIndexBuilder = new SolrSearchIndexBuilder();
        ContentProducerFactory contentProducerFactory = new ContentProducerFactory();
        SiteService siteService = mock(SiteService.class);
        BinaryEntityContentProducer binaryContentProducer = mock(BinaryEntityContentProducer.class);
        contentProducerFactory.addContentProducer(binaryContentProducer);

        solrSearchIndexBuilder.setSolrServer(solrServer);
        solrSearchIndexBuilder.setSiteService(siteService);
        solrSearchIndexBuilder.setContentProducerFactory(contentProducerFactory);

        // SiteService think that all sites have the search tool enabled
        when(siteService.getSite(anyString())).then(new Answer<Site>() {
            @Override
            public Site answer(InvocationOnMock invocationOnMock) throws Throwable {
                Site site = mock(Site.class);
                when(site.getToolForCommonId(anyString())).thenReturn(mock(ToolConfiguration.class));
                return site;
            }
        });

        for (Map.Entry<String, String> document : documents.entrySet())
            addDocument(document.getKey(), document.getValue(), solrSearchIndexBuilder, binaryContentProducer);
    }

    private void addDocument(final String reference, String contentPath,
                             SearchIndexBuilder searchIndexBuilder,
                             BinaryEntityContentProducer contentProducer) throws Exception {
        Notification notification = mock(Notification.class);
        Event event = mock(Event.class);
        when(event.getResource()).thenReturn(reference);
        when(contentProducer.matches(event)).thenReturn(true);
        when(contentProducer.getSiteId(reference)).thenReturn(reference + ".siteId");
        when(contentProducer.getContainer(reference)).thenReturn(reference + ".container");
        when(contentProducer.getId(reference)).thenReturn(reference + ".id");
        when(contentProducer.getType(reference)).thenReturn(reference + ".type");
        when(contentProducer.getSubType(reference)).thenReturn(reference + ".subtype");
        when(contentProducer.getTitle(reference)).thenReturn(reference + ".title");
        when(contentProducer.getTool()).thenReturn(reference + ".tool");
        when(contentProducer.getUrl(reference)).thenReturn(reference + ".url");
        when(contentProducer.getCustomProperties(reference)).then(new Answer<Map<String, ?>>() {
            @Override
            public Map<String, ?> answer(InvocationOnMock invocationOnMock) throws Throwable {
                return new HashMap<String, Object>() {
                    {
                        this.put("dc_author", reference + "author");
                        this.put("dc_title", reference + "title");
                        this.put("dc_description", reference + "description");
                    }
                };
            }
        });


        when(contentProducer.isContentFromReader(reference)).thenReturn(false);
        when(contentProducer.getContentStream(reference)).thenReturn(
                SolrSearchServiceTest.class.getResourceAsStream(contentPath));
        when(contentProducer.getAction(event)).thenReturn(SearchBuilderItem.ACTION_ADD);

        searchIndexBuilder.addResource(notification, event);
    }

}
