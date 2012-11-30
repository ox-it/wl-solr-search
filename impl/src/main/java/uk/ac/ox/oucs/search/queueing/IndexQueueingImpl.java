package uk.ac.ox.oucs.search.queueing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ox.oucs.search.indexing.Task;

import java.util.concurrent.Executor;

import static uk.ac.ox.oucs.search.queueing.DefaultTask.Type.INDEX_DOCUMENT;
import static uk.ac.ox.oucs.search.queueing.DefaultTask.Type.REMOVE_DOCUMENT;

/**
 * @author Colin Hebert
 */
public class IndexQueueingImpl extends WaitingTaskRunner implements IndexQueueing{
    private static final Logger logger = LoggerFactory.getLogger(IndexQueueingImpl.class);
    private Executor taskSplittingExecutor;
    private Executor indexingExecutor;

    public IndexQueueingImpl() {
        setIndexQueueing(this);
    }

    @Override
    public void addTaskToQueue(Task task) {
        if (INDEX_DOCUMENT.getTypeName().equals(task.getType()) || REMOVE_DOCUMENT.getTypeName().equals(task.getType())) {
            logger.debug("Add task '" + task + "' to the indexing executor");
            indexingExecutor.execute(new RunnableTask(task));
        } else {
            logger.debug("Add task '" + task + "' to the task splitting executor");
            taskSplittingExecutor.execute(new RunnableTask(task));
        }
    }

    public void setIndexingExecutor(Executor indexingExecutor) {
        this.indexingExecutor = indexingExecutor;
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
            runTask(task);
        }
    }
}
