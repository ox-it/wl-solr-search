package uk.ac.ox.oucs.search.solr.indexing;

import uk.ac.ox.oucs.search.queueing.DefaultTask;

import java.util.Date;

/**
 * @author Colin Gevert
 */
public class SolrTask extends DefaultTask {
    public SolrTask(Type type) {
        this(type, new Date());
    }

    public SolrTask(Type type, Date creationDate) {
        super(type.getTypeName(), creationDate);
    }

    public static enum Type {
        REMOVE_SITE_DOCUMENTS,
        REMOVE_ALL_DOCUMENTS;
        private final String typeName = Type.class.getCanonicalName() + this.toString();

        public String getTypeName() {
            return typeName;
        }
    }
}
