package org.formentor.magnolia.hollow.infrastructure;

import com.netflix.hollow.api.producer.fs.HollowFilesystemAnnouncer;
import info.magnolia.init.MagnoliaConfigurationProperties;
import jakarta.inject.Inject;
import org.formentor.magnolia.hollow.domain.HollowProducerAnnouncer;

import java.nio.file.Path;

public class HollowAnnouncerFilesystem extends HollowFilesystemAnnouncer implements HollowProducerAnnouncer {

    @Inject
    public HollowAnnouncerFilesystem(MagnoliaConfigurationProperties configuration) {
        Path publishPath = Path.of(configuration.getProperty("magnolia.home"), "joaquin_hollow");
        super(publishPath);
    }
}
