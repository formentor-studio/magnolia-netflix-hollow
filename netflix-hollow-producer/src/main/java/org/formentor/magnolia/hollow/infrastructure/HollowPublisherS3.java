package org.formentor.magnolia.hollow.infrastructure;

import com.netflix.hollow.api.producer.HollowProducer;
import com.netflix.hollow.core.memory.encoding.HashCodes;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import org.formentor.magnolia.hollow.HollowProducerModule;
import org.formentor.magnolia.hollow.domain.HollowPublisher;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class HollowPublisherS3 implements HollowPublisher {
    private final S3AsyncClient s3;
    private final HollowProducerModule definition;

    @Inject
    public HollowPublisherS3(HollowProducerModule definition) {
        this.definition = definition;
        s3 = S3AsyncClient.builder()
                .region(Region.US_EAST_1)
                .build();
    }

    @PreDestroy
    public void destroy() {
        s3.close();
    }

    @Override
    // For backwards compatability.
    public void publish(HollowProducer.Blob blob) {
        publishBlob(blob);
    }

    @Override
    public void publish(HollowProducer.PublishArtifact publishArtifact) {
        if (publishArtifact instanceof HollowProducer.HeaderBlob) {
            publishHeader((HollowProducer.HeaderBlob) publishArtifact);
        } else {
            publishBlob((HollowProducer.Blob) publishArtifact);
        }
    }

    private void publishHeader(HollowProducer.HeaderBlob headerBlob) {
        String objectName = getS3ObjectName("header", headerBlob.getVersion());

        Map<String, String> metadata = Map.of(
                "to_state", String.valueOf(headerBlob.getVersion())
        );

        uploadFile(headerBlob, objectName, metadata).join();
    }

    private void publishBlob(HollowProducer.Blob blob) {
        switch(blob.getType()) {
            case SNAPSHOT:
                publishSnapshot(blob);
                break;
            case DELTA:
            case REVERSE_DELTA:
                publishDelta(blob);
                break;
        }
    }

    private void publishSnapshot(HollowProducer.Blob blob) {
        String objectName = getS3ObjectName(blob.getType(), blob.getToVersion());

        Map<String, String> metadata = Map.of(
                "to_state", String.valueOf(blob.getToVersion())
        );

        uploadFile(blob, objectName, metadata).join();
    }

    private void publishDelta(HollowProducer.Blob blob) {
        String objectName = getS3ObjectName(blob.getType(), blob.getFromVersion());

        Map<String, String> metadata = Map.of(
                "from_state", String.valueOf(blob.getFromVersion()),
                "to_state", String.valueOf(blob.getToVersion())
        );

        uploadFile(blob, objectName, metadata).join();
    }

    private CompletableFuture<Void> uploadFile(HollowProducer.PublishArtifact publishArtifact, String key, Map<String, String> metadata) {
        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(definition.getBucketName())
                .key(key)
                .metadata(metadata)
                .build();
        try {
            InputStream inputStream = Files.newInputStream(publishArtifact.getPath());
            Long contentLength = Files.size(publishArtifact.getPath());
            CompletableFuture<?> response = s3.putObject(objectRequest, AsyncRequestBody.fromInputStream(inputStream, contentLength));

            return response.thenAccept(ignored -> {});
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getS3ObjectName(HollowProducer.Blob.Type fileType, long lookupVersion) {
        return getS3ObjectName(fileType.prefix, lookupVersion);
    }

    public String getS3ObjectName(String fileType, long lookupVersion) {
        String prefix = fileType + "/";
        return prefix + Integer.toHexString(HashCodes.hashLong(lookupVersion)) + "-" + lookupVersion;
    }
}
