package com.crokoking.datastore.export.translator;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.EmbeddedEntity;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.GeoPt;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.PropertyContainer;
import com.google.appengine.api.datastore.Text;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

@SuppressWarnings("rawtypes")
public class EntityJsonWriter {
    private final JsonWriter jsonWriter;

    public EntityJsonWriter(JsonWriter jsonWriter) {
        this.jsonWriter = jsonWriter;
    }

    public void serializeEntity(Entity entity) throws IOException {
        jsonWriter.beginObject();
        jsonWriter.name("key");
        serializeKey(entity.getKey());
        jsonWriter.name("properties");
        serializeProperties(entity);
        jsonWriter.endObject();
    }

    public void serializeEntities(List<Entity> entities) throws IOException {
        jsonWriter.beginArray();
        for (Entity entity : entities) {
            serializeEntity(entity);
        }
        jsonWriter.endArray();
    }

    public void serializeEmbeddedEntity(EmbeddedEntity entity) throws IOException {
        jsonWriter.beginObject();
        jsonWriter.name("key");
        serializeKey(entity.getKey());
        jsonWriter.name("properties");
        serializeProperties(entity);
        jsonWriter.endObject();
    }

    private void serializeProperties(PropertyContainer container) throws IOException {
        jsonWriter.beginArray();
        for (Map.Entry<String, Object> entry : container.getProperties().entrySet()) {
            serializeProperty(container, entry.getKey(), entry.getValue());
        }
        jsonWriter.endArray();
    }

    public void serializeProperty(PropertyContainer entity, String key, Object value) throws IOException {
        jsonWriter.beginObject();
        jsonWriter.name("key").value(key);
        if (entity.isUnindexedProperty(key)) {
            jsonWriter.name("unindexed").value(true);
        }
        serializePropertyValue(value);
        jsonWriter.endObject();
    }

    private void serializePropertyValue(Object value) throws IOException {
        if (value != null) {
            if (value instanceof Collection) {
                jsonWriter.name("class").value(Collection.class.getSimpleName());
                jsonWriter.name("value");
                jsonWriter.beginArray();
                for (Object element : (Collection) value) {
                    jsonWriter.beginObject();
                    serializePropertyValue(element);
                    jsonWriter.endObject();
                }
                jsonWriter.endArray();
            } else {
                jsonWriter.name("class").value(value.getClass().getSimpleName());
                jsonWriter.name("value");
                serializeValue(value);
            }
        } else {
            jsonWriter.name("class").value("null");
            jsonWriter.name("value").nullValue();
        }
    }

    public void serializeValue(Object value) throws IOException {
        if (value == null) {
            jsonWriter.nullValue();
        } else if (value instanceof Boolean) {
            jsonWriter.value((Boolean)value);
        } else if (value instanceof String) {
            jsonWriter.value((String)value);
        } else if (value instanceof Integer || value instanceof Long) {
            jsonWriter.value(((Number)value).longValue());
        } else if (value instanceof Float || value instanceof Double) {
            final long longBits = Double.doubleToLongBits(((Number) value).doubleValue());
            jsonWriter.value(longBits);
        } else if (value instanceof Text) {
            jsonWriter.value(((Text) value).getValue());
        } else if (value instanceof Date) {
            jsonWriter.value(((Date) value).getTime());
        } else if (value instanceof Key) {
            serializeKey((Key) value);
        } else if (value instanceof EmbeddedEntity) {
            serializeEmbeddedEntity((EmbeddedEntity) value);
        } else if (value instanceof Blob) {
            String base64 = Base64.getUrlEncoder().encodeToString(((Blob) value).getBytes());
            jsonWriter.value(base64);
        } else if (value instanceof GeoPt) {
            float latitude = ((GeoPt) value).getLatitude();
            float longitude = ((GeoPt) value).getLongitude();
            jsonWriter.beginObject();
            jsonWriter.name("latitude").value(Double.doubleToLongBits(latitude));
            jsonWriter.name("longitude").value(Double.doubleToLongBits(longitude));
            jsonWriter.endObject();
        } else {
            throw new IllegalArgumentException("Cannot serialize $value with class " + value.getClass());
        }
    }

    public void serializeKey(Key key) throws IOException {
        if (key == null) {
            jsonWriter.nullValue();
            return;
        }
        jsonWriter.beginObject();
        jsonWriter.name("keyString").value(KeyFactory.keyToString(key));
        jsonWriter.name("kind").value(key.getKind());
        if (key.getName() != null) {
            jsonWriter.name("name").value(key.getName());
        } else {
            jsonWriter.name("id").value(key.getId());
        }
        if (key.getParent() != null) {
            jsonWriter.name("parent");
            serializeKey(key.getParent());
        }
        jsonWriter.endObject();
    }
}
