package org.formentor.magnolia.hollow;

import com.netflix.hollow.api.codegen.HollowAPIGenerator;
import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.api.consumer.fs.HollowFilesystemAnnouncementWatcher;
import com.netflix.hollow.api.consumer.fs.HollowFilesystemBlobRetriever;
import com.netflix.hollow.api.producer.HollowProducer;
import com.netflix.hollow.api.producer.fs.HollowFilesystemAnnouncer;
import com.netflix.hollow.api.producer.fs.HollowFilesystemPublisher;
import com.netflix.hollow.core.write.HollowWriteStateEngine;
import com.netflix.hollow.core.write.objectmapper.HollowObjectMapper;
import com.netflix.hollow.core.write.objectmapper.HollowPrimaryKey;
import org.formentor.magnolia.hollow.api.MovieAPI;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

public class ProducerTest {
    @Disabled("Integration test")
    @Test
    void publish() throws IOException {
        Path publishDir = Path.of(System.getProperty("user.dir"), "joaquin_hollow");
        System.out.println(String.format("Hollow data folder '%s'", publishDir));

        //
        // 1. Create producer to Hollow
        //
        HollowProducer producer = createProducer(publishDir);

        //
        // 2. Add data to Hollow
        //
        var movies = getMovies();
        producer.runCycle(state -> {
            for(Movie movie : movies)
                state.add(movie);
        });

        //
        // 3. Generate API classes
        //
        ProducerTest.generateApi();

        //
        // 4. Create consumer
        //
        HollowConsumer consumer = createConsumer(publishDir);

        //
        // 5. Consume data
        //
        consumer.triggerRefresh();
        MovieAPI movieApi = (MovieAPI)consumer.getAPI();
        movieApi.getAllMovie().forEach(movie -> System.out.println(movie.getId() + ", " +
                movie.getTitle().getValue() + ", " +
                movie.getReleaseYear()));
    }

    private HollowProducer createProducer(Path publishDir) {
        HollowFilesystemPublisher publisher = new HollowFilesystemPublisher(publishDir);
        HollowFilesystemAnnouncer announcer = new HollowFilesystemAnnouncer(publishDir);
        return HollowProducer
                .withPublisher(publisher)
                .withAnnouncer(announcer)
                .build();
    }

    private HollowConsumer createConsumer(Path publishDir) {
        HollowFilesystemBlobRetriever blobRetriever =
                new HollowFilesystemBlobRetriever(publishDir);

        HollowFilesystemAnnouncementWatcher announcementWatcher =
                new HollowFilesystemAnnouncementWatcher(publishDir);

         return HollowConsumer.withBlobRetriever(blobRetriever)
                .withAnnouncementWatcher(announcementWatcher)
                .withGeneratedAPIClass(MovieAPI.class)
                .build();

    }
    @HollowPrimaryKey(fields="id")
    private static class Movie {
        long id;
        String title;
        int releaseYear;

        public Movie(long id, String title, int releaseYear) {
            this.id = id;
            this.title = title;
            this.releaseYear = releaseYear;
        }
    }

    private List<Movie> getMovies() {
        return Arrays.asList(
                new Movie(1, "The Matrix", 1999),
                new Movie(2, "Beasts of No Nation", 2015),
                new Movie(3, "Pulp Fiction", 1994)
        );
    }

    private static void generateApi() throws IOException {
        HollowWriteStateEngine writeEngine = new HollowWriteStateEngine();
        HollowObjectMapper mapper = new HollowObjectMapper(writeEngine);
        mapper.initializeTypeState(Movie.class);

        HollowAPIGenerator generator =
                new HollowAPIGenerator.Builder().withAPIClassname("MovieAPI")
                        .withDestination(Path.of(System.getProperty("user.dir"), "src/test/java"))
                        .withPackageName("org.formentor.magnolia.hollow.api")
                        .withDataModel(writeEngine)
                        .build();

        generator.generateSourceFiles();
    }
}
