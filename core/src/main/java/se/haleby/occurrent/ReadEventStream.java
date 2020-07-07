package se.haleby.occurrent;

import io.cloudevents.CloudEvent;

public interface ReadEventStream {
    // TODO Add time as parameter and make this method a default method with time = now
    default EventStream<CloudEvent> read(String streamId) {
        return read(streamId, 0, Integer.MAX_VALUE);
    }

    EventStream<CloudEvent> read(String streamId, int skip, int limit);
}
