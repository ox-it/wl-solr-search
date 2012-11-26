package uk.ac.ox.oucs.search.queueing;

import uk.ac.ox.oucs.search.indexing.IndexProcesses;

import java.util.Date;

/**
 * @author Colin Hebert
 */
public class DefaultTask implements Task {
    private Date actionDate;
    private String resourceName;
    private String siteId;
    private TaskType taskType;

    public Date getActionDate() {
        return actionDate;
    }

    public void setActionDate(Date actionDate) {
        this.actionDate = actionDate;
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
                indexProcesses.indexDocument(resourceName, actionDate);
                break;
            case REMOVE_DOCUMENT:
                indexProcesses.removeDocument(resourceName, actionDate);
                break;
            case INDEX_SITE:
                indexProcesses.indexSite(siteId, actionDate);
                break;
            case REFRESH_SITE:
                indexProcesses.refreshSite(siteId, actionDate);
                break;
            case INDEX_ALL:
                indexProcesses.indexAll(actionDate);
                break;
            case REFRESH_ALL:
                indexProcesses.refreshAll(actionDate);
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
