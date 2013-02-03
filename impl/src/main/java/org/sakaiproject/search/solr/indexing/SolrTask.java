package org.sakaiproject.search.solr.indexing;

import org.sakaiproject.search.indexing.DefaultTask;

import java.util.Date;

/**
 * Tasks specific to a Solr search index (such as optimise, remove site documents and remove all documents).
 *
 * @author Colin Hebert
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
        REMOVE_ALL_DOCUMENTS,
        OPTIMISE_INDEX;
        private final String typeName = Type.class.getCanonicalName() + '.' + this.toString();

        public String getTypeName() {
            return typeName;
        }
    }
}
