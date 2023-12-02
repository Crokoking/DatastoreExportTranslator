package com.crokoking.datastore.export.translator;

import com.google.appengine.api.datastore.Entity;
import com.google.gson.stream.JsonReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class ValidatorMain {
    public static void main(String[] args) {
        final Option help = Option.builder().option("help").desc("Print help").build();
        final Option input = Option.builder().option("input").hasArgs().desc("Input file").required().build();

        final Options options = new Options();
        options.addOption(help);
        options.addOption(input);

        final CommandLine commandLine;
        try {
            final DefaultParser parser = new DefaultParser();
            commandLine = parser.parse(options, args);
        } catch (ParseException e) {
            printHelp(options);
            return;
        }

        if (commandLine.hasOption(help)) {
            printHelp(options);
            return;
        }

        final String inputPathString = commandLine.getOptionValue(input);
        final Path inputPath = Path.of(inputPathString);
        if (!Files.exists(inputPath)) {
            System.err.println("Input directory does not exist: " + inputPathString);
            return;
        }
        if (!Files.isRegularFile(inputPath)) {
            System.err.println("Input file is not a file: " + inputPathString);
            return;
        }

        try(FileReader fr = new FileReader(inputPath.toFile())) {
            final BufferedReader bufferedReader = new BufferedReader(fr);
            final JsonReader jsonReader = new JsonReader(bufferedReader);
            jsonReader.beginArray();
            long validated = 0;
            final EntityJsonReader entityJsonReader = new EntityJsonReader(jsonReader);
            while (jsonReader.hasNext()) {
                final Entity entity = entityJsonReader.deserializeEntity();
                validated++;
                if(validated % 1000 == 0) {
                    System.out.println("Validated " + validated + " entities");
                }
            }
            System.out.println("Validated " + validated + " entities");
            jsonReader.endArray();
        } catch (IOException e) {
            e.printStackTrace(System.err);
        }
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("DatastoreExportTranslator", options, true);
    }
}