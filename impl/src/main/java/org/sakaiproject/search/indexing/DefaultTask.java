package org.sakaiproject.search.indexing;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Colin Hebert
 */
public class DefaultTask implements Task {
    public final static String REFERENCE = DefaultTask.class.getCanonicalName() + ".reference";
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

    @Override
    public String toString() {
        return "DefaultTask{" +
                "type='" + type + '\'' +
                ", creationDate=" + creationDate +
                ", properties=" + properties +
                '}';
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
