package com.crokoking.datastore.export.translator;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityTranslator;
import com.google.appengine.repackaged.com.google.io.protocol.ProtocolSource;
import com.google.gson.stream.JsonWriter;
import com.google.storage.onestore.v3.OnestoreEntity;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Translator {
    public void translate(Path directoryPath, Path outputPath, List<String> includeKinds, List<String> excludeKinds) throws IOException {

        try (final FileWriter fileWriter = new FileWriter(outputPath.toFile())) {
            try(final BufferedWriter writer = new BufferedWriter(fileWriter)) {
                try(final JsonWriter jsonWriter = new JsonWriter(writer)) {
                    jsonWriter.setIndent(" ");
                    translate(directoryPath, includeKinds, excludeKinds, jsonWriter);
                }
            }
        }
    }

    private void translate(Path directoryPath, List<String> includeKinds, List<String> excludeKinds, JsonWriter jsonWriter) throws IOException {
        final EntityJsonWriter entityJsonWriter = new EntityJsonWriter(jsonWriter);
        jsonWriter.beginArray();
        final AtomicLong counter = new AtomicLong();
        System.out.println("Translating backup from " + directoryPath);
        LevelDBLogParser parser = new LevelDBLogParser(directoryPath, bytes -> {
            Entity entity = toEntity(bytes);
            if (!includeKinds.isEmpty() && !includeKinds.contains(entity.getKind())) {
                return;
            }
            if (excludeKinds.contains(entity.getKind())) {
                return;
            }
            try {
                entityJsonWriter.serializeEntity(entity);
            } catch (IOException e) {
                e.printStackTrace(System.err);
            }
            final long counterValue = counter.incrementAndGet();
            if (counterValue % 1000 == 0) {
                System.out.println("Translated " + counterValue + " entities");
            }
        });
        parser.parse();
        jsonWriter.endArray();
        System.out.println("Translated " + counter.get() + " entities");
    }

    private Entity toEntity(byte[] bytes) {
        final OnestoreEntity.EntityProto entityProto = new OnestoreEntity.EntityProto();
        ProtocolSource protocolSource = new ProtocolSource(bytes);
        entityProto.merge(protocolSource);
        return EntityTranslator.createFromPb(entityProto);
    }
}