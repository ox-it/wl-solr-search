package uk.ac.ox.oucs.search.indexing;

import uk.ac.ox.oucs.search.queueing.DefaultTask;

import java.util.Date;

import static uk.ac.ox.oucs.search.queueing.DefaultTask.Type.*;

/**
 * @author Colin Hebert
 */
public abstract class AbstractTaskHandler implements TaskHandler {
    @Override
    public void executeTask(Task task) {
        String taskType = task.getType();
        if (INDEX_DOCUMENT.getTypeName().equals(taskType)) {
            indexDocument(task.getProperty(DefaultTask.RESOURCE_NAME), task.getCreationDate());
        } else if (REMOVE_DOCUMENT.getTypeName().equals(taskType)) {
            removeDocument(task.getProperty(DefaultTask.RESOURCE_NAME), task.getCreationDate());
        } else if (INDEX_SITE.getTypeName().equals(taskType)) {
            indexSite(task.getProperty(DefaultTask.SITE_ID), task.getCreationDate());
        } else if (REFRESH_SITE.getTypeName().equals(taskType)) {
            refreshSite(task.getProperty(DefaultTask.SITE_ID), task.getCreationDate());
        } else if (INDEX_ALL.getTypeName().equals(taskType)) {
            indexAll(task.getCreationDate());
        } else if (REFRESH_ALL.getTypeName().equals(taskType)) {
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
