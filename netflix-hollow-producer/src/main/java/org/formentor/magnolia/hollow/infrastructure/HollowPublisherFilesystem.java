package org.formentor.magnolia.hollow.infrastructure;

import com.netflix.hollow.api.producer.fs.HollowFilesystemPublisher;
import info.magnolia.init.MagnoliaConfigurationProperties;
import jakarta.inject.Inject;
import org.formentor.magnolia.hollow.domain.HollowProducerPublisher;

import java.nio.file.Path;

public class HollowPublisherFilesystem extends HollowFilesystemPublisher implements HollowProducerPublisher {

    @Inject
    public HollowPublisherFilesystem(MagnoliaConfigurationProperties configuration) {
        Path blobStorePath = Path.of(configuration.getProperty("magnolia.home"), "joaquin_hollow");
        super(blobStorePath);
    }
}
