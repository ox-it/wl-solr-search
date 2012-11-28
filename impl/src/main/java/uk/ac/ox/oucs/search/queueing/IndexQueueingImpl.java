package uk.ac.ox.oucs.search.queueing;

import org.sakaiproject.tool.api.Session;
import org.sakaiproject.tool.api.SessionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ox.oucs.search.indexing.Task;
import uk.ac.ox.oucs.search.indexing.TaskHandler;
import uk.ac.ox.oucs.search.indexing.exception.TemporaryTaskHandlingException;

import java.util.concurrent.Executor;

import static uk.ac.ox.oucs.search.queueing.DefaultTask.Type.INDEX_DOCUMENT;
import static uk.ac.ox.oucs.search.queueing.DefaultTask.Type.REMOVE_DOCUMENT;

/**
 * @author Colin Hebert
 */
public class IndexQueueingImpl implements IndexQueueing {
    private static final Logger logger = LoggerFactory.getLogger(IndexQueueingImpl.class);
    private TaskHandler taskHandler;
    private Executor taskSplittingExecutor;
    private Executor indexingExecutor;
    private SessionManager sessionManager;

    @Override
    public void addTaskToQueue(Task task) {
        if (INDEX_DOCUMENT.getTypeName().equals(task.getType()) || REMOVE_DOCUMENT.getTypeName().equals(task.getType()))
            indexingExecutor.execute(new RunnableTask(task));
        else
            taskSplittingExecutor.execute(new RunnableTask(task));
    }

    public void setIndexingExecutor(Executor indexingExecutor) {
        this.indexingExecutor = indexingExecutor;
    }

    public void setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    public void setTaskHandler(TaskHandler taskHandler) {
        this.taskHandler = taskHandler;
    }

    public void setTaskSplittingExecutor(Executor taskSplittingExecutor) {
        this.taskSplittingExecutor = taskSplittingExecutor;
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
                taskHandler.executeTask(task);
            } catch (TemporaryTaskHandlingException e) {
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
