package uk.ac.ox.oucs.search.indexing;

import uk.ac.ox.oucs.search.queueing.DefaultTask;

import java.util.Date;

/**
 * @author Colin Hebert
 */
public abstract class AbstractTaskHandler implements TaskHandler {
    @Override
    public void executeTask(Task task) {
        String taskType = task.getType();
        if (DefaultTask.Type.INDEX_DOCUMENT.equals(taskType)) {
            indexDocument(task.getProperty(DefaultTask.RESOURCE_NAME), task.getCreationDate());
        } else if (DefaultTask.Type.REMOVE_DOCUMENT.equals(taskType)) {
            removeDocument(task.getProperty(DefaultTask.RESOURCE_NAME), task.getCreationDate());
        } else if (DefaultTask.Type.INDEX_SITE.equals(taskType)) {
            indexSite(task.getProperty(DefaultTask.SITE_ID), task.getCreationDate());
        } else if (DefaultTask.Type.REFRESH_SITE.equals(taskType)) {
            refreshSite(task.getProperty(DefaultTask.SITE_ID), task.getCreationDate());
        } else if (DefaultTask.Type.INDEX_ALL.equals(taskType)) {
            indexAll(task.getCreationDate());
        } else if (DefaultTask.Type.REFRESH_ALL.equals(taskType)) {
            refreshAll(task.getCreationDate());
        }
    }

    protected abstract void indexDocument(String resourceName, Date actionDate);

    protected abstract void removeDocument(String resourceName, Date actionDate);

    protected abstract void indexSite(String siteId, Date actionDate);

    protected abstract void refreshSite(String siteId, Date actionDate);

    protected abstract void indexAll(Date actionDate);

    protected abstract void refreshAll(Date actionDate);
}
