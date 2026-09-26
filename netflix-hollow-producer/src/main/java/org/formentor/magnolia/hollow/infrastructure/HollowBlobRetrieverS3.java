package org.formentor.magnolia.hollow.infrastructure;

import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.api.producer.HollowProducer;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import org.formentor.magnolia.hollow.HollowProducerModule;
import org.formentor.magnolia.hollow.domain.HollowBlobRetriever;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class HollowBlobRetrieverS3 implements HollowBlobRetriever {
    private final HollowProducerModule definition;
    private final HollowPublisherS3 hollowPublisherS3;
    private final S3AsyncClient s3;

    @Inject
    public HollowBlobRetrieverS3(HollowProducerModule definition, HollowPublisherS3 hollowPublisherS3) {
        this.definition = definition;
        this.hollowPublisherS3 = hollowPublisherS3;
        s3 = S3AsyncClient.builder()
                .region(Region.US_EAST_1)
                .build();
    }

    @PreDestroy
    public void destroy() {
        s3.close();
    }

    @Override
    public HollowConsumer.HeaderBlob retrieveHeaderBlob(long desiredVersion) {
        String objectKey = hollowPublisherS3.getS3ObjectName("header", desiredVersion);

        return new HeaderBlobS3(objectKey, desiredVersion);
    }

    @Override
    public HollowConsumer.Blob retrieveSnapshotBlob(long desiredVersion) {
        String objectKey = hollowPublisherS3.getS3ObjectName(HollowProducer.Blob.Type.SNAPSHOT, desiredVersion);

        return new BlobS3(objectKey, desiredVersion);
    }

    @Override
    public HollowConsumer.Blob retrieveDeltaBlob(long currentVersion) {
        String objectKey = hollowPublisherS3.getS3ObjectName(HollowProducer.Blob.Type.DELTA, currentVersion);
        Map<String, String> metadata = getS3Metadata(objectKey);
        long fromState = Long.parseLong(metadata.get("from_state"));
        long toState = Long.parseLong(metadata.get("to_state"));

        return new BlobS3(objectKey, fromState, toState);
    }

    @Override
    public HollowConsumer.Blob retrieveReverseDeltaBlob(long currentVersion) {
        String objectKey = hollowPublisherS3.getS3ObjectName(HollowProducer.Blob.Type.REVERSE_DELTA, currentVersion);
        Map<String, String> metadata = getS3Metadata(objectKey);
        long fromState = Long.parseLong(metadata.get("from_state"));
        long toState = Long.parseLong(metadata.get("to_state"));

        return new BlobS3(objectKey, fromState, toState);
    }

    private class HeaderBlobS3 extends HollowConsumer.HeaderBlob {
        private final String objectKey;
        protected HeaderBlobS3(String objectKey, long version) {
            super(version);
            this.objectKey = objectKey;
        }

        @Override
        public InputStream getInputStream() {
            return getS3Object(objectKey);
        }
    }

    private class BlobS3 extends HollowConsumer.Blob {
        private final String objectKey;

        public BlobS3(String objectKey, long toVersion) {
            super(toVersion);
            this.objectKey = objectKey;
        }

        public BlobS3(String objectKey, long fromVersion, long toVersion) {
            super(fromVersion, toVersion);
            this.objectKey = objectKey;
        }

        @Override
        public InputStream getInputStream() throws IOException {
            return getS3Object(objectKey);
        }
    }

    private InputStream getS3Object(String objectKey) {
        GetObjectRequest objectRequest = GetObjectRequest.builder()
                .key(objectKey)
                .bucket(definition.getBucketName())
                .build();
        try {
            return s3.getObject(objectRequest, AsyncResponseTransformer.toBlockingInputStream()).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, String> getS3Metadata(String objectKey) {
        HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                .bucket(definition.getBucketName())
                .key(objectKey)
                .build();

        try {
            return s3.headObject(headObjectRequest).thenApply(HeadObjectResponse::metadata).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
