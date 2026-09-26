package org.formentor.magnolia.hollow.infrastructure;

import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import org.formentor.magnolia.hollow.HollowProducerModule;
import org.formentor.magnolia.hollow.domain.HollowAnnouncer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.util.Map;

public class HollowAnnouncerDynamoDB implements HollowAnnouncer {
    private final DynamoDbAsyncClient ddb;
    private final HollowProducerModule definition;

    @Inject
    public HollowAnnouncerDynamoDB(HollowProducerModule definition) {
        this.definition = definition;
        ddb = DynamoDbAsyncClient.builder()
                .region(Region.US_EAST_1)
                .build();
    }

    @PreDestroy
    public void destroy() {
        ddb.close();
    }

    @Override
    public void announce(long stateVersion) {
        Map<String, AttributeValue> itemValues = Map.of(
                "property", AttributeValue.builder().s("version").build(),
                "value", AttributeValue.builder().n(String.valueOf(stateVersion)).build()
        );
        PutItemRequest request = PutItemRequest.builder()
                .tableName(definition.getTableName())
                .item(itemValues)
                .build();

        ddb.putItem(request).join();
    }
}
