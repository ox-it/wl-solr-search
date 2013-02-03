package org.sakaiproject.search.indexing;

import java.io.Serializable;
import java.util.Date;

/**
 * Task created whenever a new operation related to the index occurs.
 * <p>
 * A task contains the minimal amount of information that will be used when executed by a {@link TaskHandler}.
 * </p>
 *
 * @author Colin Hebert
 */
public interface Task extends Serializable {
    String getType();

    String getProperty(String name);

    Date getCreationDate();
}
