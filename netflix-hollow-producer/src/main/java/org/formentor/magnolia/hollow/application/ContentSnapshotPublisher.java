package org.formentor.magnolia.hollow.application;

import com.google.common.collect.ImmutableMap;
import com.machinezoo.noexception.Exceptions;
import com.netflix.hollow.api.consumer.fs.HollowFilesystemAnnouncementWatcher;
import com.netflix.hollow.api.consumer.fs.HollowFilesystemBlobRetriever;
import com.netflix.hollow.api.producer.HollowProducer;
import com.netflix.hollow.api.producer.fs.HollowFilesystemAnnouncer;
import com.netflix.hollow.api.producer.fs.HollowFilesystemPublisher;
import com.netflix.hollow.core.schema.HollowObjectSchema;
import com.netflix.hollow.core.write.HollowObjectWriteRecord;
import info.magnolia.config.registry.DefinitionProvider;
import info.magnolia.init.MagnoliaConfigurationProperties;
import info.magnolia.types.ContentTypeDefinition;
import info.magnolia.types.ContentTypeRegistry;
import info.magnolia.types.datasource.jcr.JcrDataSourceDefinition;
import info.magnolia.types.model.PropertyDefinition;
import jakarta.inject.Inject;
import org.formentor.magnolia.hollow.domain.HollowProducerAnnouncer;
import org.formentor.magnolia.hollow.domain.HollowProducerPublisher;

import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ContentSnapshotPublisher {
    private static final String KEY_FIELD_NAME = "id"; // TODO Rename to "_id" and check if it is supported by Hollow
    private static final Map<String, HollowObjectSchema.FieldType> TYPE_MAPPING_HOLLOW = ImmutableMap.<String, HollowObjectSchema.FieldType>builder()
            .put("", HollowObjectSchema.FieldType.STRING)
            .put("richText", HollowObjectSchema.FieldType.STRING)
            .put("String", HollowObjectSchema.FieldType.STRING)
            .put("Boolean", HollowObjectSchema.FieldType.BOOLEAN)
            .put("Long", HollowObjectSchema.FieldType.LONG)
            .put("Decimal", HollowObjectSchema.FieldType.FLOAT)
            .put("Date", HollowObjectSchema.FieldType.LONG)
            .build();

    private static final Map<String, String> TYPE_MAPPING_MAGNOLIA = ImmutableMap.<String, String>builder()
            .put("", "String")
            .put("richText", "String")
            .put("String", "String")
            .put("Boolean", "Boolean")
            .put("Long", "Long")
            .put("Decimal", "BigDecimal")
            .put("Date", "Date")
            .build();
    private final ContentTypeRegistry contentTypeRegistry;
    private final HollowProducer producer;
    private final Map<String, HollowObjectSchema> hollowObjectSchemas = new HashMap<>(); // key: ContentType name, value: HollowObjectSchema

    @Inject
    public ContentSnapshotPublisher(MagnoliaConfigurationProperties configuration, ContentTypeRegistry contentTypeRegistry, HollowProducerPublisher publisher, HollowProducerAnnouncer announcer) {
        this.contentTypeRegistry = contentTypeRegistry;

        // Create producer
        // TODO Create HollowProducer.Incremental to support add, update and delete records!
        producer = HollowProducer
                .withPublisher(publisher)
                .withAnnouncer(announcer)
                .build();

        // Create Hollow Schemas by ContentType
        List<ContentTypeDefinition> contentTypes = getContentTypes();
        contentTypes.forEach(contentType -> hollowObjectSchemas.put(contentType.getName(), buildHollowObjectSchema(contentType)));
        HollowObjectSchema[] schemas = hollowObjectSchemas.values().toArray(new HollowObjectSchema[0]);
        producer.initializeDataModel(schemas); // It is necessary to restore current Version

        // Restore Latest Version
        Path publishDir = Path.of(configuration.getProperty("magnolia.home"), "joaquin_hollow");
        HollowFilesystemAnnouncementWatcher announcementWatcher = new HollowFilesystemAnnouncementWatcher(publishDir);
        HollowFilesystemBlobRetriever blobRetriever = new HollowFilesystemBlobRetriever(publishDir);
        long latestAnnouncedVersion = announcementWatcher.getLatestVersion();
        producer.restore(latestAnnouncedVersion, blobRetriever);
    }

    public void publish(Node contentNode) {
        List<ContentTypeDefinition> contentTypes = getContentTypes(contentNode);
        contentTypes.forEach(contentType -> {
            Optional<HollowObjectWriteRecord> hollowRecord = buildHollowRecord(contentNode, contentType);
            hollowRecord.ifPresent(writeRecord -> {
                producer.runCycle(state -> {
                    var writeEngine = state.getStateEngine();
                    writeEngine.add(contentType.getName(), writeRecord);
                });
            });
        });
    }

    private Optional<HollowObjectWriteRecord> buildHollowRecord(Node contentNode, ContentTypeDefinition contentType) {
        if (!hollowObjectSchemas.containsKey(contentType.getName())) {
            return Optional.empty();
        }
        HollowObjectSchema hollowObjectSchema = hollowObjectSchemas.get(contentType.getName());
        HollowObjectWriteRecord hollowRecord = new HollowObjectWriteRecord(hollowObjectSchema);
        hollowRecord.setString(KEY_FIELD_NAME, Exceptions.wrap().get(contentNode::getIdentifier));
        List<PropertyDefinition> propertiesSupported = getPropertiesSupported(contentType);
        propertiesSupported.forEach(propertyDefinition -> {
            Exceptions.wrap().run(() -> {
                getPropertyValue(contentNode, propertyDefinition).ifPresent(value -> {
                    HollowObjectSchema.FieldType type = TYPE_MAPPING_HOLLOW.get(propertyDefinition.getType());
                    switch (type) {
                        case STRING -> hollowRecord.setString(propertyDefinition.getName(), value.toString());
                        case BOOLEAN -> hollowRecord.setBoolean(propertyDefinition.getName(), (Boolean) value);
                        case LONG -> hollowRecord.setLong(propertyDefinition.getName(), (Long) value);
                        case FLOAT ->  hollowRecord.setFloat(propertyDefinition.getName(), (Float) value);
                    }
                });
            });
        });
        return Optional.of(hollowRecord);
    }

    private Optional<Object> getPropertyValue(Node node, PropertyDefinition propertyDefinition) throws RepositoryException {
        String propertyName = propertyDefinition.getName();
        if (!node.hasProperty(propertyName)) {
            return Optional.empty();
        }
        String type = TYPE_MAPPING_MAGNOLIA.get(propertyDefinition.getType());
        if (type == null) {
            return Optional.empty();
        }
        Property property = node.getProperty(propertyName);
        return switch (type) {
            case "String" -> Optional.ofNullable(property.getString());
            case "Boolean" -> Optional.of(property.getBoolean());
            case "Long" -> Optional.of(property.getLong());
            case "BigDecimal" -> Optional.ofNullable(property.getDecimal()).map(BigDecimal::doubleValue);
            case "Date" -> Optional.ofNullable(property.getDate()).map(calendar -> calendar.getTimeInMillis());
            default -> throw new IllegalArgumentException();
        };
    }

    private List<ContentTypeDefinition> getContentTypes() {
        return contentTypeRegistry.getAllProviders().stream()
                .filter(DefinitionProvider::isValid)
                .filter(provider -> provider.getProblems().stream().noneMatch(problem -> problem.getSeverityType() == DefinitionProvider.Problem.SeverityType.SEVERE))
                .map(DefinitionProvider::get)
                .filter(contentTypeDefinition -> contentTypeDefinition.getModel() != null)
                .toList();
    }

    private List<ContentTypeDefinition> getContentTypes(Node contentNode) {
        String workspace = Exceptions.wrap().get(() -> contentNode.getSession().getWorkspace().getName());
        return getContentTypes().stream()
                .filter(contentTypeDefinition -> workspace.equals(((JcrDataSourceDefinition)contentTypeDefinition.getDatasource()).getWorkspace()))
                .toList();
    }

    private HollowObjectSchema buildHollowObjectSchema(ContentTypeDefinition contentType) {
        List<PropertyDefinition> propertiesSupported = getPropertiesSupported(contentType);
        int numFields = propertiesSupported.size() + 1 ; // Add the "id" field
        HollowObjectSchema hollowObjectSchema = new HollowObjectSchema(contentType.getName(), numFields, KEY_FIELD_NAME);
        hollowObjectSchema.addField(KEY_FIELD_NAME, HollowObjectSchema.FieldType.STRING);
        propertiesSupported.forEach(propertyDefinition -> {
            hollowObjectSchema.addField(propertyDefinition.getName(), TYPE_MAPPING_HOLLOW.get(propertyDefinition.getType()));
        });

        return hollowObjectSchema;
    }

    private List<PropertyDefinition> getPropertiesSupported(ContentTypeDefinition contentType) {
        return contentType.getModel().getProperties().stream().filter(propertyDefinition -> TYPE_MAPPING_HOLLOW.containsKey(propertyDefinition.getType())).toList();
    }

    private HollowProducer.Publisher createHollowFilesystemPublisher(Path publishDir) {
        return new HollowFilesystemPublisher(publishDir);
    }

    private HollowProducer.Announcer createHollowFilesystemAnnouncer(Path publishDir) {
        return new HollowFilesystemAnnouncer(publishDir);
    }
}
