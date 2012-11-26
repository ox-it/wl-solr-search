package uk.ac.ox.oucs.search.indexing;

import uk.ac.ox.oucs.search.indexing.IndexProcesses;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

/**
 * @author Colin Hebert
 */
public interface Task extends Serializable {
    String getType();
    String getProperty(String name);
    Date getCreationDate();
}
