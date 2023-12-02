package com.crokoking.datastore.export.translator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        final Option help = Option.builder().option("help").desc("Print help").build();
        final Option input = Option.builder().option("input").hasArgs().desc("Input directory").required().build();
        final Option output = Option.builder().option("output").hasArgs().desc("Output file").required().build();
        final Option include = Option.builder().option("include").hasArgs().desc("Include kind. If any are specified, only included kinds are exported").build();
        final Option exclude = Option.builder().option("exclude").hasArgs().desc("Exclude kind").build();

        final Options options = new Options();
        options.addOption(help);
        options.addOption(input);
        options.addOption(output);
        options.addOption(include);
        options.addOption(exclude);

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
        if (!Files.isDirectory(inputPath)) {
            System.err.println("Input directory is not a directory: " + inputPathString);
            return;
        }
        final String outputFileString = commandLine.getOptionValue(output);
        final Path outputPath = Path.of(outputFileString);
        if (Files.exists(outputPath) && !Files.isRegularFile(outputPath)) {
            System.err.println("Output file is not a file: " + outputFileString);
            return;
        }
        if (Files.exists(outputPath.getParent())) {
            if(!Files.isDirectory(outputPath.getParent())) {
                System.err.println("Output file parent is not a directory: " + outputFileString);
                return;
            }
        } else {
            final boolean mkdirs = outputPath.getParent().toFile().mkdirs();
            if(!mkdirs) {
                System.err.println("Could not create output file parent: " + outputFileString);
                return;
            }
        }

        List<String> includeKinds = new ArrayList<>();
        if (commandLine.hasOption(include)) {
            final String[] values = commandLine.getOptionValues(include);
            Collections.addAll(includeKinds, values);
        }

        List<String> excludeKinds = new ArrayList<>();
        if (commandLine.hasOption(exclude)) {
            final String[] values = commandLine.getOptionValues(exclude);
            Collections.addAll(excludeKinds, values);
        }

        final Translator translator = new Translator();
        try {
            translator.translate(inputPath, outputPath, includeKinds, excludeKinds);
        } catch (IOException e) {
            System.err.println("Error while parsing: " + e.getMessage());
        }
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("DatastoreExportTranslator", options, true);
    }
}