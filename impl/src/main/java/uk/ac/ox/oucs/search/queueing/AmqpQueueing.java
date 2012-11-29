package uk.ac.ox.oucs.search.queueing;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ox.oucs.search.indexing.Task;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class AmqpQueueing implements IndexQueueing {
    private static final Logger logger = LoggerFactory.getLogger(AmqpQueueing.class);
    private Connection amqpConnection;
    private String queueName;

    public void destroy() {
        try {
            amqpConnection.close();
        } catch (IOException e) {
            logger.error("Exception while closing the connection to the AMQP server", e);
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
            logger.error("An exception occured during the serialization of '" + task + "'", e);
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

    public void setAmqpConnection(Connection amqpConnection) {
        this.amqpConnection = amqpConnection;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }
}
