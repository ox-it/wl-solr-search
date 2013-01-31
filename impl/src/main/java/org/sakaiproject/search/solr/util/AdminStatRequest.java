package org.sakaiproject.search.solr.util;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;

import java.io.IOException;
import java.util.Collection;

/**
 * SolrRequest used to get results from {@link org.apache.solr.handler.admin.SolrInfoMBeanHandler}
 * <p>
 * By default, {@link AdminStatRequest} considers that {@link org.apache.solr.handler.admin.SolrInfoMBeanHandler} is
 * mapped on "/admin/stats"
 * </p>
 *
 * @author Colin Hebert
 */
public class AdminStatRequest extends SolrRequest {
    private final ModifiableSolrParams params = new ModifiableSolrParams();

    public AdminStatRequest() {
        this("/admin/stats");
    }

    public AdminStatRequest(String path) {
        super(METHOD.GET, path);
        params.add("stats", "true");
    }

    public void setParam(String param, String value) {
        params.set(param, value);
    }

    @Override
    public SolrParams getParams() {
        return params;
    }

    @Override
    public Collection<ContentStream> getContentStreams() throws IOException {
        return null;
    }

    @Override
    public SolrResponse process(SolrServer server) throws SolrServerException, IOException {
        SolrResponse response = new SolrResponseBase();
        response.setResponse(server.request(this));
        return response;
    }
}
