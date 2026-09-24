package org.formentor.magnolia.hollow;

import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.api.consumer.fs.HollowFilesystemAnnouncementWatcher;
import com.netflix.hollow.api.consumer.fs.HollowFilesystemBlobRetriever;
import com.netflix.hollow.api.producer.HollowProducer;
import com.netflix.hollow.api.producer.fs.HollowFilesystemAnnouncer;
import com.netflix.hollow.api.producer.fs.HollowFilesystemPublisher;
import com.netflix.hollow.core.read.dataaccess.HollowObjectTypeDataAccess;
import com.netflix.hollow.core.read.engine.object.HollowObjectTypeReadState;
import com.netflix.hollow.core.schema.HollowObjectSchema;
import com.netflix.hollow.core.write.HollowObjectWriteRecord;
import com.netflix.hollow.core.write.HollowWriteStateEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ProducerWithSchemaTest {

    private HollowProducer producer;
    private HollowConsumer consumer;
    private HollowObjectSchema tourSchema;

    @BeforeEach
    public void setUp() {
        //
        // 1. Create producer
        //
        Path publishDir = Path.of(System.getProperty("user.dir"), "joaquin_hollow");
        HollowFilesystemPublisher publisher = new HollowFilesystemPublisher(publishDir);
        HollowFilesystemAnnouncer announcer = new HollowFilesystemAnnouncer(publishDir);
        producer = HollowProducer
                .withPublisher(publisher)
                .withAnnouncer(announcer)
                .build();
        //
        // 2. Define schema
        //
        tourSchema = new HollowObjectSchema("Tour", 9, "id");
        tourSchema.addField("id", HollowObjectSchema.FieldType.STRING);
        tourSchema.addField("name", HollowObjectSchema.FieldType.STRING);
        tourSchema.addField("description", HollowObjectSchema.FieldType.STRING);
        tourSchema.addField("isFeatured", HollowObjectSchema.FieldType.BOOLEAN);
        tourSchema.addField("location", HollowObjectSchema.FieldType.STRING);
        tourSchema.addField("date", HollowObjectSchema.FieldType.LONG);
        tourSchema.addField("duration", HollowObjectSchema.FieldType.LONG);
        tourSchema.addField("author", HollowObjectSchema.FieldType.STRING);
        tourSchema.addField("body", HollowObjectSchema.FieldType.STRING);

        //
        // 3. Create consumer and get elements
        //
        HollowFilesystemBlobRetriever blobRetriever =
                new HollowFilesystemBlobRetriever(publishDir);
        HollowFilesystemAnnouncementWatcher announcementWatcher =
                new HollowFilesystemAnnouncementWatcher(publishDir);
        consumer = HollowConsumer.withBlobRetriever(blobRetriever)
                .withAnnouncementWatcher(announcementWatcher)
                .build();
    }
    @Disabled("Integration test")
    @Test
    void produce() {
        producer.initializeDataModel(tourSchema);

        //
        // 1. Add elements
        //
        producer.runCycle(state -> {
            HollowWriteStateEngine engine = state.getStateEngine();

            HollowObjectWriteRecord tourRec = new HollowObjectWriteRecord(tourSchema);
            tourRec.setString("id", "3d676477-eabc-4cbe-88b6-b77aa85a358a");
            tourRec.setString("name", "Vietnam: Tradition and Today");
            tourRec.setString("description", "Discover the culture and everyday treasures of a rising phoenix");
            tourRec.setBoolean("isFeatured", true);
            tourRec.setString("location", "Ho Chi Minh City, Vietnam");
            tourRec.setLong("date", 1790180096970L);
            tourRec.setLong("duration", 14L);
            tourRec.setString("author", "Magnolia Travel");
            tourRec.setString("body", "<p>Vietnam is one of the world&rsquo;s most exotic and culturally rich destinations. A gem among gems, it offers dazzling diversity for visitors. Yet what most find so fascinating is its capacity for change. This is nowhere more evident than in Ho Chi Minh City. Set on the Saigon river, the capital is the perfect example of progress and tradition living side-by-side. Grand colonial buildings stand alongside modern skyscrapers while Japanese cars share the busy roads with cycle rickshaws.</p><p>We won&rsquo;t just take you to the key sights, but also reveal the hidden stories that explain what makes Vietnam what it is today, and where it&rsquo;s going.</p><p>A two day tour of the Mekong delta (Vietnamese: Đồng bằng S&ocirc;ng Cửu Long &quot;Nine Dragon river delta&quot;) will immerse you in a water-world maze where everything happens on the boats - even local markets which form every day from a raft of vendors boats. We&rsquo;ll continue to the coastal town of Vung Tau in the Phuong district to relax and swim like the locals do.</p><p>While there we&rsquo;ll visit the storied White Palace, the historical old colonial villa of the french governor.</p><p>Along the way, you&rsquo;ll eat some of the finest food you&rsquo;ve ever eaten, and we might even teach you to cook it too.</p> ");
            engine.add("Tour", tourRec);
        });

        //
        // 2. Consume elements
        //
        consumer.triggerRefresh(); // Retrieve last version
        var dataAccess = ((HollowObjectTypeDataAccess)consumer.getAPI().getDataAccess().getTypeDataAccess("Tour"));

        String id = dataAccess.readString(0, 0);
        String name = dataAccess.readString(0, 1);
        Boolean isFeatured = dataAccess.readBoolean(0, 3);
        long date = dataAccess.readLong(0, 5);
        assertEquals("3d676477-eabc-4cbe-88b6-b77aa85a358a", id);
        assertEquals("Vietnam: Tradition and Today", name);
        assertEquals(true, isFeatured);
        assertEquals(1790180096970L, date);

        int maxOrdinal = ((HollowObjectTypeReadState) dataAccess).maxOrdinal();
        for (int i = 0; i <= maxOrdinal; i++) {
            for (int f = 0; f < tourSchema.numFields(); f++) {
                System.out.print(f + "(" + tourSchema.getFieldName(f) + ")" + ":");
                HollowObjectSchema.FieldType type = tourSchema.getFieldType(f);
                switch (type) {
                    case STRING -> System.out.println(dataAccess.readString(i, f));
                    case BOOLEAN -> System.out.println(dataAccess.readBoolean(i, f));
                    case LONG -> System.out.println(dataAccess.readLong(i, f));
                    case FLOAT ->  System.out.println(dataAccess.readFloat(i, f));
                }
            }
            System.out.println();
        }
    }

    @Test
    void consume() {
        var publishDir = Path.of("/Users/joaquinalfaroramonell/Documents/Projects/magnolia-netflix-hollow/30-Bin/magnolia-data/joaquin_hollow");
        HollowFilesystemBlobRetriever blobRetriever =
                new HollowFilesystemBlobRetriever(publishDir);
        HollowFilesystemAnnouncementWatcher announcementWatcher =
                new HollowFilesystemAnnouncementWatcher(publishDir);
        consumer = HollowConsumer.withBlobRetriever(blobRetriever)
                .withAnnouncementWatcher(announcementWatcher)
                .build();

        consumer.triggerRefresh(); // Retrieve last version
        var dataAccess = ((HollowObjectTypeDataAccess)consumer.getAPI().getDataAccess().getTypeDataAccess("tour"));
        int maxOrdinal = ((HollowObjectTypeReadState) dataAccess).maxOrdinal();
        for (int i = 0; i <= maxOrdinal; i++) {
            for (int f = 0; f < tourSchema.numFields(); f++) {
                System.out.print(f + "(" + tourSchema.getFieldName(f) + ")" + ":");
                HollowObjectSchema.FieldType type = tourSchema.getFieldType(f);
                switch (type) {
                    case STRING -> System.out.println(dataAccess.readString(i, f));
                    case BOOLEAN -> System.out.println(dataAccess.readBoolean(i, f));
                    case LONG -> System.out.println(dataAccess.readLong(i, f));
                    case FLOAT ->  System.out.println(dataAccess.readFloat(i, f));
                }
            }
            System.out.println();
        }
    }
}
