package org.sakaiproject.search.indexing;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Default implementation of Task providing the basic operations that are generated by the search tool.
 * <p>
 * Basic operations are enumerated in {@link Type}.
 * </p>
 *
 * @author Colin Hebert
 */
public class DefaultTask implements Task {
    /**
     * Unique reference to an element within sakai that should be indexed.
     */
    public static final String REFERENCE = DefaultTask.class.getCanonicalName() + ".reference";
    /**
     * Identifier of a site to index/reindex/cleanup.
     */
    public static final String SITE_ID = DefaultTask.class.getCanonicalName() + ".siteId";
    private final String type;
    private final Date creationDate;
    private final Map<String, String> properties = new HashMap<String, String>();

    /**
     * Builds a task of the given type.
     * <p>
     * The creation date will be generated automatically.
     * </p>
     *
     * @param type type of task.
     */
    public DefaultTask(Type type) {
        this(type, new Date());
    }

    /**
     * Builds a task of the specified type.
     *
     * @param type         type of the task.
     * @param creationDate creation date of the task.
     */
    public DefaultTask(Type type, Date creationDate) {
        this(type.getTypeName(), creationDate);
    }

    /**
     * Builds a task with a manually given type.
     * <p>
     * This constructor is used through inheritance, allowing the creation of custom task types.
     * </p>
     *
     * @param type         type of the task.
     * @param creationDate creation date of the task.
     */
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

    /**
     * Adds a property to the task.
     * <p>
     * The usual properties expected here are {@link #REFERENCE} and {@link #SITE_ID}.
     * </p>
     *
     * @param name  name of the property.
     * @param value value expected for the property.
     * @return the current task, allowing chained calls.
     */
    public DefaultTask setProperty(String name, String value) {
        properties.put(name, value);
        return this;
    }

    @Override
    public String toString() {
        return "DefaultTask{"
                + "type='" + type + '\''
                + ", creationDate=" + creationDate
                + ", properties=" + properties
                + '}';
    }

    /**
     * Task type with an automatically generated name (based on the class name) to avoid collisions.
     */
    public static enum Type {
        /**
         * Type of a task in charge of indexing an unique document.
         */
        INDEX_DOCUMENT,
        /**
         * Type of a task in charge of removing an unique document from the index.
         */
        REMOVE_DOCUMENT,

        /**
         * Type of a task in charge of indexing a complete site.
         */
        INDEX_SITE,
        /**
         * Type of a task in charge of refreshing a complete site.
         * <p>
         * Refreshing a site consists in reindexing already indexed documents (new documents won't be indexed)
         * and deleting old documents.
         * </p>
         */
        REFRESH_SITE,


        /**
         * Type of a task in charge of rebuilding the entire index.
         */
        INDEX_ALL,
        /**
         * Type of a task in charge of refreshing the entire index.
         * <p>
         * Refreshing the index site consists in reindexing already indexed documents (new documents won't be indexed)
         * and deleting old documents.
         * </p>
         */
        REFRESH_ALL;
        private final String typeName = Type.class.getCanonicalName() + '.' + this.toString();

        public String getTypeName() {
            return typeName;
        }
    }
}
