package com.crokoking.datastore.export.translator;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.EmbeddedEntity;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.GeoPt;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.PropertyContainer;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class EntityJsonTest {

    private final LocalServiceTestHelper helper =
        new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig());

    @BeforeEach
    public void setUp() {
        helper.setUp();
    }

    @AfterEach
    public void tearDown() {
        helper.tearDown();
    }

    @Test
    public void testSerialization() throws IOException {
        final List<Entity> inputEntities = generateTestEntities();

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        JsonWriter jsonWriter = new JsonWriter(new OutputStreamWriter(byteArrayOutputStream));
        jsonWriter.setIndent(" ");
        EntityJsonWriter entityJsonWriter = new EntityJsonWriter(jsonWriter);
        entityJsonWriter.serializeEntities(inputEntities);
        jsonWriter.close();
        byteArrayOutputStream.close();

        final byte[] byteArray = byteArrayOutputStream.toByteArray();
        final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArray);
        JsonReader jsonReader = new JsonReader(new InputStreamReader(byteArrayInputStream));
        EntityJsonReader entityJsonReader = new EntityJsonReader(jsonReader);
        final List<Entity> outputEntities = entityJsonReader.deserializeEntityArray();
        assert outputEntities.size() == inputEntities.size();
        for (int i = 0; i < inputEntities.size(); i++) {
            final Entity inputEntity = inputEntities.get(i);
            final Entity outputEntity = outputEntities.get(i);
            Assertions.assertEquals(inputEntity.getKey(), outputEntity.getKey());
            Assertions.assertEquals(inputEntity.getKind(), outputEntity.getKind());
            assertProperties(inputEntity, outputEntity);
        }
    }

    @SuppressWarnings("rawtypes")
    private void assertProperties(PropertyContainer expected, PropertyContainer actual) {
        final Map<String, Object> expectedProperties = expected.getProperties();
        final Map<String, Object> actualProperties = actual.getProperties();
        Assertions.assertEquals(expectedProperties.size(), actualProperties.size());
        Assertions.assertEquals(expectedProperties.keySet(), actualProperties.keySet());
        for (String key : expectedProperties.keySet()) {
            final Object expectedValue = expected.getProperty(key);
            final Object actualValue = actual.getProperty(key);
            assert (expectedValue != null && actualValue != null) || (expectedValue == null && actualValue == null);
            if (expectedValue != null) {
                assertSameClass(expectedValue, actualValue);
            }
            assert expected.isUnindexedProperty(key) == actual.isUnindexedProperty(key);
            if (expectedValue instanceof List) {
                final List expecctedList = (List) expectedValue;
                final List actualList = (List) actualValue;
                Assertions.assertEquals(expecctedList.size(), actualList.size());
                for (int i = 0; i < expecctedList.size(); i++) {
                    final Object expectedListValue = expecctedList.get(i);
                    final Object actualListValue = actualList.get(i);
                    assertSameClass(expectedListValue, actualListValue);
                    if (expectedListValue instanceof EmbeddedEntity) {
                        assertEmbeddedEntities((EmbeddedEntity) expectedListValue, (EmbeddedEntity) actualListValue);
                    } else {
                        Assertions.assertEquals(expectedListValue, actualListValue);
                    }
                }
            } else if (expectedValue instanceof EmbeddedEntity) {
                assertEmbeddedEntities((EmbeddedEntity) expectedValue, (EmbeddedEntity) actualValue);
            } else {
                Assertions.assertEquals(expectedValue, actualValue);
            }
        }
    }

    private static void assertSameClass(Object expectedValue, Object actualValue) {
        if (expectedValue instanceof Collection && actualValue instanceof Collection) {
            return;
        }
        Assertions.assertEquals(expectedValue.getClass(), actualValue.getClass());
    }

    private void assertEmbeddedEntities(EmbeddedEntity expectedValue, EmbeddedEntity actualValue) {
        Assertions.assertEquals(expectedValue.getKey(), actualValue.getKey());
        assertProperties(expectedValue, actualValue);
    }

    private static List<Entity> generateTestEntities() {
        Entity nameEntity = new Entity("nameEntity", "name");
        setAllFields(nameEntity);
        Entity idEntity = new Entity("idEntity", 1L);
        setAllFields(idEntity);

        return Arrays.asList(nameEntity, idEntity);
    }

    private static void setAllFields(Entity entity) {
        setEntityFields(entity);
        EmbeddedEntity embeddedEntityNoKey = new EmbeddedEntity();
        setEntityFields(embeddedEntityNoKey);
        entity.setProperty("embeddedEntityNoKey", embeddedEntityNoKey);

        EmbeddedEntity embeddedEntityIdKey = new EmbeddedEntity();
        embeddedEntityIdKey.setKey(KeyFactory.createKey("embeddedEntityIdKey", 1L));
        setEntityFields(embeddedEntityIdKey);
        entity.setProperty("embeddedEntityNoKey", embeddedEntityIdKey);

        EmbeddedEntity embeddedEntityNameKey = new EmbeddedEntity();
        embeddedEntityNameKey.setKey(KeyFactory.createKey("embeddedEntityNameKey", "name"));
        setEntityFields(embeddedEntityNameKey);
        entity.setProperty("embeddedEntityNoKey", embeddedEntityNameKey);
    }

    private static void setEntityFields(PropertyContainer entity) {
        entity.setProperty("string", "string");
        entity.setProperty("long", 1L);
        entity.setProperty("double", 1.0);
        entity.setProperty("int", 1);
        entity.setProperty("float", 1.0f);
        entity.setProperty("boolean", true);
        entity.setProperty("date", new Date());
        entity.setProperty("null", null);
        entity.setProperty("blob", new Blob(new byte[]{1, 2, 3}));
        entity.setProperty("text", new Text("text"));
        entity.setProperty("geoPt", new GeoPt(1.0f, 2.0f));
        entity.setProperty("idKey", KeyFactory.createKey("key", 1));
        entity.setProperty("nameKey", KeyFactory.createKey("key", "name"));
        entity.setProperty("ancestorIdKey", KeyFactory.createKey(KeyFactory.createKey("parent", 1L), "child", 2L));
        entity.setProperty("ancestorNameKey", KeyFactory.createKey(KeyFactory.createKey("parent", "name1"), "child", "name2"));
        List<String> stringList = Arrays.asList("string1", "string2");
        entity.setProperty("stringList", stringList);
        List<Long> longList = Arrays.asList(1L, 2L);
        entity.setProperty("longList", longList);
        List<Double> doubleList = Arrays.asList(1.0, 2.0);
        entity.setProperty("doubleList", doubleList);
        List<Integer> intList = Arrays.asList(1, 2);
        entity.setProperty("intList", intList);
        List<Float> floatList = Arrays.asList(1.0f, 2.0f);
        entity.setProperty("floatList", floatList);
        List<Boolean> booleanList = Arrays.asList(true, false);
        entity.setProperty("booleanList", booleanList);
        List<Date> dateList = Arrays.asList(new Date(), new Date());
        entity.setProperty("dateList", dateList);
        List<Blob> blobList = Arrays.asList(new Blob(new byte[]{1, 2, 3}), new Blob(new byte[]{1, 2, 3}));
        entity.setProperty("blobList", blobList);
        List<Text> textList = Arrays.asList(new Text("text1"), new Text("text2"));
        entity.setProperty("textList", textList);
        List<GeoPt> geoPtList = Arrays.asList(new GeoPt(1.0f, 2.0f), new GeoPt(1.0f, 2.0f));
        entity.setProperty("geoPtList", geoPtList);
        List<Key> idKeyList = Arrays.asList(
            KeyFactory.createKey("key", 1),
            KeyFactory.createKey("key", 2),
            KeyFactory.createKey(KeyFactory.createKey("parent", 1L), "child", 2L)
        );
        entity.setProperty("idKeyList", idKeyList);
        List<Key> nameKeyList = Arrays.asList(
            KeyFactory.createKey("key", "1"),
            KeyFactory.createKey("key", "2"),
            KeyFactory.createKey(KeyFactory.createKey("parent", "1"), "child", "2")
        );
        entity.setProperty("nameKeyList", nameKeyList);

        entity.setUnindexedProperty("uString", "string");
        entity.setUnindexedProperty("uLong", 1L);
        entity.setUnindexedProperty("uDouble", 1.0);
        entity.setUnindexedProperty("uInt", 1);
        entity.setUnindexedProperty("uFloat", 1.0f);
        entity.setUnindexedProperty("uBoolean", true);
        entity.setUnindexedProperty("uDate", new Date());
        entity.setUnindexedProperty("uNull", null);
        entity.setUnindexedProperty("uGeoPt", new GeoPt(1.0f, 2.0f));
        entity.setUnindexedProperty("uKey", KeyFactory.createKey("key", 1));
        entity.setUnindexedProperty("uStringList", stringList);
        entity.setUnindexedProperty("uLongList", longList);
        entity.setUnindexedProperty("uDoubleList", doubleList);
        entity.setUnindexedProperty("uLongList", intList);
        entity.setUnindexedProperty("uFloatList", floatList);
        entity.setUnindexedProperty("uBooleanList", booleanList);
        entity.setUnindexedProperty("uDateList", dateList);
        entity.setUnindexedProperty("uGeoPtList", geoPtList);
        entity.setUnindexedProperty("uKeyList", nameKeyList);
    }
}
