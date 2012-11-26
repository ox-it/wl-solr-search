package uk.ac.ox.oucs.search.indexing;

import java.io.Serializable;
import java.util.Date;

/**
 * @author Colin Hebert
 */
public interface Task extends Serializable {
    String getType();
    String getProperty(String name);
    Date getCreationDate();
}
