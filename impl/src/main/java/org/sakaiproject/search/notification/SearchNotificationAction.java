package org.sakaiproject.search.notification;

import org.sakaiproject.event.api.Event;
import org.sakaiproject.event.api.Notification;
import org.sakaiproject.event.api.NotificationAction;
import org.sakaiproject.search.api.SearchIndexBuilder;
import org.w3c.dom.Element;

/**
 * NotificationAction which captures events related to the search system and relay them to the {@link SearchIndexBuilder}.
 *
 * @author Colin Hebert
 */
public class SearchNotificationAction implements NotificationAction {
    private final SearchIndexBuilder searchIndexBuilder;

    public SearchNotificationAction(SearchIndexBuilder searchIndexBuilder) {
        this.searchIndexBuilder = searchIndexBuilder;
    }

    @Override
    public void set(Element element) {
    }

    @Override
    public void set(NotificationAction notificationAction) {
    }

    @Override
    public NotificationAction getClone() {
        return new SearchNotificationAction(this.searchIndexBuilder);
    }

    @Override
    public void toXml(Element element) {
    }

    @Override
    public void notify(Notification notification, Event event) {
        searchIndexBuilder.addResource(notification, event);
    }
}
