package org.formentor.magnolia.hollow.infrastructure;

import com.netflix.hollow.api.producer.fs.HollowFilesystemPublisher;
import info.magnolia.init.MagnoliaConfigurationProperties;
import jakarta.inject.Inject;
import org.formentor.magnolia.hollow.domain.HollowPublisher;

import java.nio.file.Path;

public class HollowPublisherFilesystem extends HollowFilesystemPublisher implements HollowPublisher {

    @Inject
    public HollowPublisherFilesystem(MagnoliaConfigurationProperties configuration) {
        Path blobStorePath = Path.of(configuration.getProperty("magnolia.home"), "test_netflix_hollow");
        super(blobStorePath);
    }
}
