package uk.ac.ox.oucs.search.solr.queueing;

import org.sakaiproject.tool.api.Session;
import org.sakaiproject.tool.api.SessionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ox.oucs.search.indexing.IndexProcesses;
import uk.ac.ox.oucs.search.indexing.TemporaryProcessExecutionException;

import java.util.concurrent.ExecutorService;

/**
 * @author Colin Hebert
 */
public class IndexQueueingImpl implements IndexQueueing {
    private static final Logger logger = LoggerFactory.getLogger(IndexQueueingImpl.class);
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
                indexProcesses.executeTask(task);
            } catch (TemporaryProcessExecutionException e) {
                logger.warn("The task '" + task + "' couldn't be executed, try again later.", e);
                addTaskToQueue(task);
            } catch (Exception e) {
                logger.error("Couldn't execute task '" + task + "'.", e);
            }
        }

        private void logAsAdmin() {
            Session session = sessionManager.getCurrentSession();
            session.setUserId("admin");
            session.setUserEid("admin");
        }
    }
}
