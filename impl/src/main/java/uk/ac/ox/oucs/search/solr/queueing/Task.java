package uk.ac.ox.oucs.search.solr.queueing;

import java.util.Date;

/**
 * @author Colin Hebert
 */
public class Task {
    private Date requestDate;
    private String resourceName;
    private String siteId;
    private TaskType taskType;

    public Date getRequestDate() {
        return requestDate;
    }

    public void setRequestDate(Date requestDate) {
        this.requestDate = requestDate;
    }

    public String getResourceName() {
        return resourceName;
    }

    public void setResourceName(String resourceName) {
        this.resourceName = resourceName;
    }

    public String getSiteId() {
        return siteId;
    }

    public void setSiteId(String siteId) {
        this.siteId = siteId;
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public void setTaskType(TaskType taskType) {
        this.taskType = taskType;
    }

    public static enum TaskType {
        INDEX_DOCUMENT,
        REINDEX_DOCUMENT,
        UNINDEX_DOCUMENT,

        INDEX_SITE,
        REINDEX_SITE,
        UNINDEX_SITE,

        INDEX_ALL,
        REINDEX_ALL,
        UNINDEX_ALL
    }

}
