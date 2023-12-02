package com.crokoking.datastore.export.translator;

import org.iq80.leveldb.impl.LogMonitor;
import org.iq80.leveldb.impl.LogReader;
import org.iq80.leveldb.util.Slice;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Consumer;
import java.util.stream.Stream;

class LevelDBLogParser {
    private final Path directoryPath;
    private final Consumer<byte[]> recordConsumer;

    public LevelDBLogParser(Path directoryPath, Consumer<byte[]> recordConsumer) {
        this.directoryPath = directoryPath;
        this.recordConsumer = recordConsumer;
    }

    public void parse() throws IOException {
        try(final Stream<Path> stream = Files.walk(directoryPath)) {
            stream.filter(path -> path.getFileName().toString().startsWith("output-")).forEach(this::parseFile);
        }
    }

    private void parseFile(Path path) {
        try {
            doParseFile(path);
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }

    private void doParseFile(Path path) {
        System.out.println("Importing from file " + path);
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ)) {
            final LogReader logReader = new LogReader(fileChannel, new LogMonitor() {
                @Override
                public void corruption(long bytes, String reason) {
                    System.err.println("corruption: " + reason);
                }

                @Override
                public void corruption(long bytes, Throwable reason) {
                    System.err.println("corruption: ");
                    reason.printStackTrace(System.err);
                }
            }, true, 0);
            do {
                while(true) {
                    Slice slice = logReader.readRecord();
                    if(slice == null) {
                        break;
                    }
                    recordConsumer.accept(slice.getBytes());
                }
            } while (logReader.readNextBlock());
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }
}
