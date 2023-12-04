package com.crokoking.datastore.export.translator;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.EmbeddedEntity;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.PropertyContainer;
import com.google.appengine.api.datastore.Text;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@SuppressWarnings("unused")
public class EntityJsonReader {
    private final JsonReader jsonReader;

    public EntityJsonReader(JsonReader jsonReader) {
        this.jsonReader = jsonReader;
    }

    public List<Entity> deserializeEntityArray() throws IOException {
        List<Entity> output = new ArrayList<>();
        final AtomicLong read = new AtomicLong();
        deserializeEntityArray((entity -> {
            output.add(entity);
            final long cur = read.incrementAndGet();
            if (cur % 1000 == 0) {
                System.out.println("Read " + cur + " entities");
            }
        }));
        System.out.println("Validated " + read.get() + " entities");
        jsonReader.endArray();
        return output;
    }

    public void deserializeEntityArray(Consumer<Entity> consumer) throws IOException {
        jsonReader.beginArray();
        final EntityJsonReader entityJsonReader = new EntityJsonReader(jsonReader);
        while (jsonReader.hasNext()) {
            final Entity entity = entityJsonReader.deserializeEntity();
            consumer.accept(entity);
        }
        jsonReader.endArray();
    }

    public Entity deserializeEntity() throws IOException {
        jsonReader.beginObject();
        skipToName("key");
        Key key = deserializeKey();
        Entity entity = new Entity(key);
        skipToName("properties");
        deserializeProperties(entity);
        jsonReader.endObject();
        return entity;
    }

    public Object deserializeValue(String valueClass) throws IOException {
        if (valueClass == null || "null".equals(valueClass)) {
            jsonReader.nextNull();
            return null;
        } else if (String.class.getSimpleName().equals(valueClass)) {
            return jsonReader.nextString();
        } else if (Boolean.class.getSimpleName().equals(valueClass)) {
            return jsonReader.nextBoolean();
        } else if (Integer.class.getSimpleName().equals(valueClass)) {
            return jsonReader.nextInt();
        } else if (Long.class.getSimpleName().equals(valueClass)) {
            return jsonReader.nextLong();
        } else if (Float.class.getSimpleName().equals(valueClass)) {
            return ((Double) deserializeValue(Double.class.getSimpleName())).floatValue();
        } else if (Double.class.getSimpleName().equals(valueClass)) {
            if (jsonReader.peek() == JsonToken.STRING) {
                final String str = jsonReader.nextString();
                if (str.startsWith("D")) { //Old format
                    final long longBits = Long.parseLong(str.substring(1), 32);
                    return Double.longBitsToDouble(longBits);
                }
            }
            final long longBits = jsonReader.nextLong();
            return Double.longBitsToDouble(longBits);
        } else if (Text.class.getSimpleName().equals(valueClass)) {
            return new Text(jsonReader.nextString());
        } else if (Date.class.getSimpleName().equals(valueClass)) {
            return new Date(jsonReader.nextLong());
        } else if (Key.class.getSimpleName().equals(valueClass)) {
            return deserializeKey();
        } else if (EmbeddedEntity.class.getSimpleName().equals(valueClass)) {
            return deserializeEmbeddedEntity();
        } else if (Collection.class.getSimpleName().equals(valueClass)) {
            return deserializeCollection();
        } else if (Blob.class.getSimpleName().equals(valueClass)) {
            return new Blob(Base64.getUrlDecoder().decode(jsonReader.nextString()));
        } else {
            throw new IllegalArgumentException("Cannot deserialize class " + valueClass);
        }
    }

    public Object deserializeCollection() throws IOException {
        List<Object> result = new ArrayList<>();
        jsonReader.beginArray();
        while (jsonReader.hasNext()) {
            Object deserialized = deserializePropertyValue();
            result.add(deserialized);
        }
        jsonReader.endArray();
        return result;
    }

    private void skipToName(String name) throws IOException {
        while (jsonReader.hasNext()) {
            if (name.equals(jsonReader.nextName())) {
                return;
            } else {
                jsonReader.skipValue();
            }
        }
    }

    public EmbeddedEntity deserializeEmbeddedEntity() throws IOException {
        jsonReader.beginObject();
        EmbeddedEntity entity = new EmbeddedEntity();
        skipToName("key");
        Key key = deserializeKey();
        entity.setKey(key);
        skipToName("properties");
        deserializeProperties(entity);
        skipToObjectEnd();
        jsonReader.endObject();
        return entity;
    }

    private void deserializeProperties(PropertyContainer container) throws IOException {
        jsonReader.beginArray();
        while (jsonReader.hasNext()) {
            deserializeProperty(container);
        }
        jsonReader.endArray();
    }

    private void deserializeProperty(PropertyContainer container) throws IOException {
        jsonReader.beginObject();
        String key = null;
        Object value = null;
        boolean unindexed = false;
        String valueClass = null;
        boolean foundValue = false;
        while (jsonReader.hasNext()) {
            String name = jsonReader.nextName();
            if ("key".equals(name)) {
                key = jsonReader.nextString();
            } else if ("unindexed".equals(name)) {
                unindexed = jsonReader.nextBoolean();
            } else if ("class".equals(name)) {
                valueClass = jsonReader.nextString();
            } else if ("value".equals(name)) {
                value = deserializeValue(valueClass);
                foundValue = true;
            } else {
                jsonReader.skipValue();
            }
        }
        if (key == null) {
            throw new IllegalArgumentException("Missing key");
        }
        if (!foundValue && valueClass != null && !"null".equals(valueClass)) {
            throw new IllegalArgumentException("Missing value");
        }
        setEntityProperty(container, key, unindexed, value);
        jsonReader.endObject();
    }

    private Object deserializePropertyValue() throws IOException {
        jsonReader.beginObject();
        String valueClass = null;
        Object value = null;
        while (jsonReader.hasNext()) {
            String name = jsonReader.nextName();
            if ("class".equals(name)) {
                valueClass = jsonReader.nextString();
            } else if ("value".equals(name)) {
                value = deserializeValue(valueClass);
                break;
            } else {
                throw new IllegalArgumentException("Unknown property " + name);
            }
        }
        skipToObjectEnd();
        jsonReader.endObject();
        return value;
    }

    private void setEntityProperty(PropertyContainer container, String key, boolean unindexed, Object value) {
        if (unindexed) {
            container.setUnindexedProperty(key, value);
        } else {
            container.setProperty(key, value);
        }
    }

    public Key deserializeKey() throws IOException {
        if (jsonReader.peek() == JsonToken.NULL) {
            jsonReader.nextNull();
            return null;
        }
        jsonReader.beginObject();
        skipToName("keyString");
        String keyString = jsonReader.nextString();
        Key key = KeyFactory.stringToKey(keyString);
        skipToObjectEnd();
        jsonReader.endObject();
        return key;
    }

    private void skipToObjectEnd() throws IOException {
        while (jsonReader.hasNext()) {
            jsonReader.nextName();
            jsonReader.skipValue();
        }
    }
}
