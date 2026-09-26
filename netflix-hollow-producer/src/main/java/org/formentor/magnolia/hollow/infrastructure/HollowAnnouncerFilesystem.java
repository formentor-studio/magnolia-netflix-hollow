package org.formentor.magnolia.hollow.infrastructure;

import com.netflix.hollow.api.producer.fs.HollowFilesystemAnnouncer;
import info.magnolia.init.MagnoliaConfigurationProperties;
import jakarta.inject.Inject;
import org.formentor.magnolia.hollow.domain.HollowAnnouncer;

import java.nio.file.Path;

public class HollowAnnouncerFilesystem extends HollowFilesystemAnnouncer implements HollowAnnouncer {

    @Inject
    public HollowAnnouncerFilesystem(MagnoliaConfigurationProperties configuration) {
        Path publishPath = Path.of(configuration.getProperty("magnolia.home"), "test_netflix_hollow");
        super(publishPath);
    }
}
