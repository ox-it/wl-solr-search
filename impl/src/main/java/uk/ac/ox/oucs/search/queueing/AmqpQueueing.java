package uk.ac.ox.oucs.search.queueing;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ox.oucs.search.indexing.Task;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class AmqpQueueing implements IndexQueueing {
    private static final Logger logger = LoggerFactory.getLogger(AmqpQueueing.class);
    private ConnectionFactory connectionFactory;
    private Connection amqpConnection;
    private String queueName;
    private boolean running = true;

    public void init() {
        try {
            amqpConnection = connectionFactory.newConnection();
        } catch (IOException e) {
            logger.error("Exception while closing the connection to the AMQP server", e);
        }
    }

    public void destroy() {
        synchronized (this) {
            try {
                running = false;
                amqpConnection.close();
            } catch (IOException e) {
                logger.error("Exception while closing the connection to the AMQP server", e);
            }
        }
    }

    @Override
    public void addTaskToQueue(Task task) {
        Channel channel = null;
        try {
            channel = amqpConnection.createChannel();
            channel.basicPublish("", queueName, null, serialize(task));
        } catch (IOException e) {
            logger.error("Exception while sending a task to the AMQP server", e);
        } finally {
            try {
                if (channel != null)
                    channel.close();
            } catch (Exception e) {
                logger.error("Couldn't close the channel", e);
            }
        }
    }

    private byte[] serialize(Task task) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = null;
        try {
            oos = new ObjectOutputStream(baos);
            oos.writeObject(task);
        } catch (IOException e) {
            logger.error("An exception occurred during the serialization of '" + task + "'", e);
        } finally {
            try {
                if (oos != null)
                    oos.close();
            } catch (Exception e) {
                logger.error("Couldn't close the stream", e);
            }
        }
        return baos.toByteArray();
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    private class AmqpHandlerShutdownListener implements ShutdownListener {

        @Override
        public void shutdownCompleted(ShutdownSignalException cause) {
            synchronized (this) {
                //TODO: Avoid looping reconnection?
                if (running)
                    init();
            }
        }
    }
}
