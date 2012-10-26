package uk.ac.ox.oucs.search.solr.process;

import org.sakaiproject.component.cover.ComponentManager;
import org.sakaiproject.tool.api.Session;
import org.sakaiproject.tool.api.SessionManager;

/**
 * @author Colin Hebert
 */
public class ProcessRunner implements Runnable{
    private final SolrProcess process;
    private final SessionManager sessionManager;

    public ProcessRunner(SolrProcess process) {
        this.process = process;
        this.sessionManager = (SessionManager) ComponentManager.get(SessionManager.class);
    }

    @Override
    public void run() {
        Session session = sessionManager.getCurrentSession();
        session.setUserId("admin");
        session.setUserEid("admin");

        process.execute();
    }
}
