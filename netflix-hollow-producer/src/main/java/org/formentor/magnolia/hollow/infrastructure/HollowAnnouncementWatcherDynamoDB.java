package org.formentor.magnolia.hollow.infrastructure;

import com.netflix.hollow.api.consumer.HollowConsumer;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import org.formentor.magnolia.hollow.HollowProducerModule;
import org.formentor.magnolia.hollow.domain.HollowAnnouncementWatcher;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class HollowAnnouncementWatcherDynamoDB implements HollowAnnouncementWatcher, AutoCloseable {

    private final List<HollowConsumer> subscribedConsumers;
    private final DynamoDbAsyncClient ddb;
    private final HollowProducerModule definition;
    private long latestVersion;
    private final ScheduledExecutorService executor;
    private final ScheduledFuture<?> watchFuture;

    @Inject
    public HollowAnnouncementWatcherDynamoDB(HollowProducerModule definition) {
        this.definition = definition;
        subscribedConsumers = new CopyOnWriteArrayList<>();
        ddb = DynamoDbAsyncClient.builder()
                .region(Region.US_EAST_1)
                .build();
        latestVersion = readLatestVersion();

        executor = Executors.newScheduledThreadPool(1, r -> {
            var t = new Thread(r, "hollow-dynamodb-announcementwatcher-poller");
            t.setDaemon(true);
            return t;
        });

        watchFuture = executor.scheduleAtFixedRate(buildWatcher(), 0, 1, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        watchFuture.cancel(true);
        executor.shutdownNow();
    }

    @PreDestroy
    public void destroy() {
        close();
    }

    private Runnable buildWatcher() {
        return () -> {
            long currentVersion = readLatestVersion();
            if (latestVersion != currentVersion) {
                latestVersion = currentVersion;
                for (HollowConsumer consumer : subscribedConsumers)
                    consumer.triggerAsyncRefresh();
            }
        };
    }
    @Override
    public long getLatestVersion() {
        return readLatestVersion();
    }

    @Override
    public void subscribeToUpdates(HollowConsumer consumer) {
        subscribedConsumers.add(consumer);
    }

    private long readLatestVersion() {
        Map<String, AttributeValue> keyToGet = Map.of(
                "property", AttributeValue.builder().s("version").build()
        );
        GetItemRequest request = GetItemRequest.builder()
                .key(keyToGet)
                .tableName(definition.getTableName())
                .build();

        try {
            Map<String, AttributeValue> item = ddb.getItem(request).get().item();
            if (!item.containsKey("value")) {
                return NO_ANNOUNCEMENT_AVAILABLE;
            }
            return Long.parseLong(item.get("value").n());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
