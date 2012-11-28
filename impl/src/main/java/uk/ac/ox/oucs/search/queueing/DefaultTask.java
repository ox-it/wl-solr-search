package uk.ac.ox.oucs.search.queueing;

import uk.ac.ox.oucs.search.indexing.Task;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Colin Hebert
 */
public class DefaultTask implements Task {
    public final static String RESOURCE_NAME = DefaultTask.class.getCanonicalName() + ".resourceName";
    public final static String SITE_ID = DefaultTask.class.getCanonicalName() + ".siteId";
    private final String type;
    private final Date creationDate;
    private final Map<String, String> properties = new HashMap<String, String>();

    public DefaultTask(Type type) {
        this(type, new Date());
    }

    public DefaultTask(Type type, Date creationDate) {
        this(type.getTypeName(), creationDate);
    }

    protected DefaultTask(String type, Date creationDate) {
        this.type = type;
        this.creationDate = creationDate;
    }

    @Override
    public Date getCreationDate() {
        return creationDate;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public String getProperty(String name) {
        return properties.get(name);
    }

    public DefaultTask setProperty(String name, String value) {
        properties.put(name, value);
        return this;
    }

    public static enum Type {
        INDEX_DOCUMENT,
        REMOVE_DOCUMENT,

        INDEX_SITE,
        REFRESH_SITE,

        INDEX_ALL,
        REFRESH_ALL;
        private final String typeName = Type.class.getCanonicalName() + '.' + this.toString();

        public String getTypeName() {
            return typeName;
        }
    }
}
