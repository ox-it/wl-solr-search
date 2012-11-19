package uk.ac.ox.oucs.search.solr.queueing;

import org.sakaiproject.tool.api.Session;
import org.sakaiproject.tool.api.SessionManager;
import uk.ac.ox.oucs.search.solr.indexing.IndexProcesses;

import java.util.concurrent.ExecutorService;

/**
 * @author Colin Hebert
 */
public class IndexQueueingImpl implements IndexQueueing {
    private IndexProcesses indexProcesses;
    private ExecutorService indexingExecutor;
    private SessionManager sessionManager;

    @Override
    public void addTaskToQueue(Task task) {
        indexingExecutor.execute(new RunnableTask(task));
    }

    public void setIndexingExecutor(ExecutorService indexingExecutor) {
        this.indexingExecutor = indexingExecutor;
    }

    public void setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    public void setIndexProcesses(IndexProcesses indexProcesses) {
        this.indexProcesses = indexProcesses;
    }

    private class RunnableTask implements Runnable {
        private final Task task;

        private RunnableTask(Task task) {
            this.task = task;
        }

        @Override
        public void run() {
            logAsAdmin();
            try {
                switch (task.getTaskType()) {
                    case INDEX_DOCUMENT:
                        indexProcesses.indexDocument(task.getResourceName());
                        break;
                    case REMOVE_DOCUMENT:
                        indexProcesses.removeDocument(task.getResourceName());
                        break;
                    case INDEX_SITE:
                        indexProcesses.indexSite(task.getSiteId());
                        break;
                    case REFRESH_SITE:
                        indexProcesses.refreshSite(task.getSiteId());
                        break;
                    case INDEX_ALL:
                        indexProcesses.indexAll();
                        break;
                    case REFRESH_ALL:
                        indexProcesses.refreshAll();
                        break;

                    default:
                        //TODO: This exception shouldn't be caught here
                        throw new RuntimeException();
                }
            } catch (Exception e) { //TODO: Retry only with relevant exceptions
                addTaskToQueue(task);
            }
        }

        private void logAsAdmin() {
            Session session = sessionManager.getCurrentSession();
            session.setUserId("admin");
            session.setUserEid("admin");
        }
    }
}
