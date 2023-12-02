package com.crokoking.datastore.export.translator;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.EmbeddedEntity;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.PropertyContainer;
import com.google.appengine.api.datastore.Text;
import com.google.gson.stream.JsonReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.List;

public class EntityJsonReader {
    private final JsonReader jsonnReader;

    public EntityJsonReader(JsonReader jsonnReader) {
        this.jsonnReader = jsonnReader;
    }

    public Object deserializeValue(String valueClass) throws IOException {
        if(valueClass == null || "null".equals(valueClass)) {
            jsonnReader.nextNull();
            return null;
        } else if(String.class.getSimpleName().equals(valueClass)) {
            return jsonnReader.nextString();
        } else if(Boolean.class.getSimpleName().equals(valueClass)) {
            return jsonnReader.nextBoolean();
        } else if(Integer.class.getSimpleName().equals(valueClass)) {
            return jsonnReader.nextInt();
        } else if(Long.class.getSimpleName().equals(valueClass)) {
            return jsonnReader.nextLong();
        } else if(Float.class.getSimpleName().equals(valueClass)) {
            final long longBits = jsonnReader.nextLong();
            return (float) Double.longBitsToDouble(longBits);
        } else if(Double.class.getSimpleName().equals(valueClass)) {
            final long longBits = jsonnReader.nextLong();
            return Double.longBitsToDouble(longBits);
        } else if(Text.class.getSimpleName().equals(valueClass)) {
            return new Text(jsonnReader.nextString());
        } else if(Date.class.getSimpleName().equals(valueClass)) {
            return new Date(jsonnReader.nextLong());
        } else if(Key.class.getSimpleName().equals(valueClass)) {
            return deserializeKey();
        } else if(EmbeddedEntity.class.getSimpleName().equals(valueClass)) {
            return deserializeEmbeddedEntity();
        } else if(Collection.class.getSimpleName().equals(valueClass)) {
            return deserializeCollection();
        } else if(Blob.class.getSimpleName().equals(valueClass)) {
            return new Blob(Base64.getUrlDecoder().decode(jsonnReader.nextString()));
        } else {
            throw new IllegalArgumentException("Cannot deserialize class " + valueClass);
        }
    }

    public Object deserializeCollection() throws IOException {
        List<Object> result = new ArrayList<>();
        jsonnReader.beginArray();
        while (jsonnReader.hasNext()) {
            Object deserialized = deserializePropertyValue();
            result.add(deserialized);
        }
        jsonnReader.endArray();
        return result;
    }

    public Entity deserializeEntity() throws IOException {
        jsonnReader.beginObject();
        skipToName("key");
        Key key = deserializeKey();
        Entity entity = new Entity(key);
        skipToName("properties");
        deserializeProperties(entity);
        jsonnReader.endObject();
        return entity;
    }

    private void skipToName(String name) throws IOException {
        while(jsonnReader.hasNext()) {
            if(name.equals(jsonnReader.nextName())) {
                return;
            } else {
                jsonnReader.skipValue();
            }
        }
    }

    public EmbeddedEntity deserializeEmbeddedEntity() throws IOException {
        EmbeddedEntity entity = new EmbeddedEntity();
        skipToName("key");
        Key key = deserializeKey();
        entity.setKey(key);
        skipToName("properties");
        deserializeProperties(entity);
        return entity;
    }

    private void deserializeProperties(PropertyContainer container) throws IOException {
        jsonnReader.beginArray();
        while (jsonnReader.hasNext()) {
            deserializeProperty(container);
        }
        jsonnReader.endArray();
    }

    private void deserializeProperty(PropertyContainer container) throws IOException {
        jsonnReader.beginObject();
        skipToName("key");
        String key = jsonnReader.nextString();
        boolean unindexed = false;
        String valueClass = null;
        while (jsonnReader.hasNext()) {
            String name = jsonnReader.nextName();
            if ("unindexed".equals(name)) {
                unindexed = jsonnReader.nextBoolean();
            } else if ("class".equals(name)) {
                valueClass = jsonnReader.nextString();
            } else if ("value".equals(name)) {
                final Object value = deserializeValue(valueClass);
                setEntityProperty(container, key, unindexed, value);
                break;
            } else {
                throw new IllegalArgumentException("Unknown property " + name);
            }
        }
        skipToObjectEnd();
        jsonnReader.endObject();
    }

    private Object deserializePropertyValue() throws IOException {
        jsonnReader.beginObject();
        String valueClass = null;
        Object value = null;
        while (jsonnReader.hasNext()) {
            String name = jsonnReader.nextName();
            if ("class".equals(name)) {
                valueClass = jsonnReader.nextString();
            } else if ("value".equals(name)) {
                value = deserializeValue(valueClass);
                break;
            } else {
                throw new IllegalArgumentException("Unknown property " + name);
            }
        }
        skipToObjectEnd();
        jsonnReader.endObject();
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
        jsonnReader.beginObject();
        skipToName("keyString");
        String keyString = jsonnReader.nextString();
        Key key = KeyFactory.stringToKey(keyString);
        skipToObjectEnd();
        jsonnReader.endObject();
        return key;
    }

    private void skipToObjectEnd() throws IOException {
        while (jsonnReader.hasNext()) {
            jsonnReader.nextName();
            jsonnReader.skipValue();
        }
    }
}
