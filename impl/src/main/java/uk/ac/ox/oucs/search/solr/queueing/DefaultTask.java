package uk.ac.ox.oucs.search.solr.queueing;

import uk.ac.ox.oucs.search.indexing.IndexProcesses;
import uk.ac.ox.oucs.search.queueing.Task;

import java.util.Date;

/**
 * @author Colin Hebert
 */
public class DefaultTask implements Task {
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

    public void execute(IndexProcesses indexProcesses) {
        switch (taskType) {
            case INDEX_DOCUMENT:
                indexProcesses.indexDocument(resourceName, requestDate);
                break;
            case REMOVE_DOCUMENT:
                indexProcesses.removeDocument(resourceName, requestDate);
                break;
            case INDEX_SITE:
                indexProcesses.indexSite(siteId, requestDate);
                break;
            case REFRESH_SITE:
                indexProcesses.refreshSite(siteId, requestDate);
                break;
            case INDEX_ALL:
                indexProcesses.indexAll(requestDate);
                break;
            case REFRESH_ALL:
                indexProcesses.refreshAll(requestDate);
                break;

            default:
                throw new IllegalArgumentException("Couldn't execute a task of type '" + taskType + "'");
        }
    }

    public static enum TaskType {
        INDEX_DOCUMENT,
        REMOVE_DOCUMENT,

        INDEX_SITE,
        REFRESH_SITE,

        INDEX_ALL,
        REFRESH_ALL
    }

}
