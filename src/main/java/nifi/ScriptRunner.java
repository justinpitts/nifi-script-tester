/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nifi;

import nifi.script.AccessibleExecuteScript;
import nifi.script.AccessibleScriptingComponentHelper;
import nifi.script.ScriptingComponentUtils;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import nifi.script.ExecuteScript;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * The main entry class for testing ExecuteScript
 */
public class ScriptRunner {

    public static String DASHED_LINE = "---------------------------------------------------------";

    private static TestRunner runner;
    private static AccessibleScriptingComponentHelper scriptingComponent;

    @Command(name="java -jar nifi-script-runner-<version>.jar ", description="Where options may include:")
    static class Options {

        @Option(names = {"-attrs"}, description = "Output flow file attributes. Defaults to false.")
        boolean outputAttributes = false;

        @Option(names = {"-content"}, description = "Output flow file contents. Defaults to false.")
        boolean outputContent = false;

        @Option(names = {"-attrfile"}, description = "Path to a properties file specifying attributes to add to incoming flow files.")
        String attrFile = "";

        @Option(names = {"-modules"}, description = "Comma-separated list of paths (files or directories) containing script modules/JARs.")
        String modulePaths = "";

        @Parameters(arity="1", paramLabel="script file", description="The script to execute.")
        String scriptPath = "";

        @Option(names = {"-input"}, description = "Send each file in the specified directory as a flow file to the script.")
        String inputFileDir = "";

        @Option(names = {"-failure"}, description = "Output information about flow files that were transferred to the failure relationship. Defaults to false.")
        boolean outputFailure = false;

        @Option(names = {"-success"}, description = "Output information about flow files that were transferred to the success relationship. Defaults to true.")
        boolean outputSuccess = true;

        @Option(names = {"-all"}, description = "Output content, attributes, etc. about flow files that were transferred to any relationship. Defaults to false.")
        boolean allOutput = false;

        @Option(names = {"-all-rels"}, description = "Output information about flow files that were transferred to any relationship. Defaults to false.")
        boolean allRelations = false;

        @Option(names = {"-no-success"}, description = "Do not output information about flow files that were transferred to the success relationship. Defaults to false.")
        boolean noSuccess = false;
    }

    private static int numFiles = 0;

    public static void main(String[] args) {

        Options options = parseCommandLine(args);

        File scriptFile = new File(options.scriptPath);
        if (!scriptFile.exists()) {
            System.err.println("Script file not found: " + args[0]);
            System.exit(2);
        }

        String extension = options.scriptPath.substring(options.scriptPath.lastIndexOf(".") + 1).toLowerCase();
        String scriptEngineName = "Groovy";
        if ("js".equals(extension)) {
            scriptEngineName = "ECMAScript";
        } else if ("py".equals(extension)) {
            scriptEngineName = "python";
        } else if ("rb".equals(extension)) {
            scriptEngineName = "ruby";
        } else if ("lua".equals(extension)) {
            scriptEngineName = "lua";
        }

        final ExecuteScript executeScript = new AccessibleExecuteScript();
        // Need to do something to initialize the properties, like retrieve the list of properties
        executeScript.getSupportedPropertyDescriptors();

        runner = TestRunners.newTestRunner(executeScript);
        scriptingComponent = (AccessibleScriptingComponentHelper) executeScript;

        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, scriptEngineName);
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, options.scriptPath);
        if (!options.modulePaths.isEmpty()) {
            runner.setProperty(ScriptingComponentUtils.MODULES, options.modulePaths);
        }

        runner.assertValid();

        // Get incoming attributes from file (if specified)
        Map<String, String> incomingAttributes = new HashMap<>();
        Path attrFilePath = Paths.get(options.attrFile);
        if (!options.attrFile.isEmpty()) {
            if (!Files.exists(attrFilePath)) {
                System.err.println("Attribute file does not exist: " + options.attrFile);
                System.exit(5);
            } else {
                loadProperties(attrFilePath).forEach((k, v) -> incomingAttributes.put(k.toString(), v.toString()));
            }
        }

        try {
            if (options.inputFileDir.isEmpty()) {
                int available = System.in.available();
                if (available > 0) {
                    InputStreamReader isr = new InputStreamReader(System.in);
                    char[] input = new char[available];
                    isr.read(input);
                    runner.enqueue(new String(input), incomingAttributes);
                }
            } else {
                // Read flow files in from the folder
                Path inputFiles = Paths.get(options.inputFileDir);
                if (!Files.exists(inputFiles)) {
                    System.err.println("Input file directory does not exist: " + options.inputFileDir);
                    System.exit(3);
                }
                if (!Files.isDirectory(inputFiles)) {
                    System.err.println("Input file location is not a directory: " + options.inputFileDir);
                    System.exit(4);
                }
                Files.walkFileTree(inputFiles, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        if (attrs.isRegularFile()) {
                            incomingAttributes.put("filename", file.getFileName().toString());
                            runner.enqueue(Files.readAllBytes(file), incomingAttributes);
                            numFiles++;
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        if (numFiles > 1) {
            runner.run(numFiles);
        } else {
            runner.run();
        }
        if (options.outputSuccess) {
            outputFlowFilesForRelationship(ExecuteScript.REL_SUCCESS, options);
        }

        if (options.outputFailure) {
            outputFlowFilesForRelationship(ExecuteScript.REL_FAILURE, options);
        }
    }

    private static Properties loadProperties(Path path) {
        Properties props = new Properties();
        try {
            props.load(Files.newBufferedReader(path));
        } catch (IOException e) {
            System.err.println(String.format("Could not read properties file: %s, reason: %s",path, e.getLocalizedMessage()));
            System.exit(5);
        }
        return props;
    }

    private static Options parseCommandLine(String[] args) {
        Options options = new Options();
        try {
            options = CommandLine.populateCommand(new Options(), args);
        } catch (RuntimeException e) {
            CommandLine.usage(new Options(), System.err);
            System.exit(1);
        }

        if(options.allOutput) {
            options.outputAttributes = true;
            options.outputContent = true;
            options.outputSuccess = true;
            options.outputFailure = true;
        }

        if(options.allRelations) {
            options.outputSuccess = true;
            options.outputFailure = true;
        }

        if(options.noSuccess) {
            options.outputSuccess = false;
        }
        return options;
    }

    private static void outputFlowFilesForRelationship(Relationship relationship, Options options) {

        List<MockFlowFile> files = runner.getFlowFilesForRelationship(relationship);
        if (files != null) {
            for (MockFlowFile flowFile : files) {
                if (options.outputAttributes) {
                    final StringBuilder message = new StringBuilder();
                    message.append("Flow file ").append(flowFile);
                    message.append("\n");
                    message.append(DASHED_LINE);
                    message.append("\nFlowFile Attributes");
                    message.append(String.format("\nKey: '%1$s'\n\tValue: '%2$s'", "entryDate", new Date(flowFile.getEntryDate())));
                    message.append(String.format("\nKey: '%1$s'\n\tValue: '%2$s'", "lineageStartDate", new Date(flowFile.getLineageStartDate())));
                    message.append(String.format("\nKey: '%1$s'\n\tValue: '%2$s'", "fileSize", flowFile.getSize()));
                    message.append("\nFlowFile Attribute Map Content");
                    for (final String key : flowFile.getAttributes().keySet()) {
                        message.append(String.format("\nKey: '%1$s'\n\tValue: '%2$s'", key, flowFile.getAttribute(key)));
                    }
                    message.append("\n");
                    message.append(DASHED_LINE);
                    System.out.println(message.toString());
                }
                if (options.outputContent) {
                    System.out.println(new String(flowFile.toByteArray()));
                }
                System.out.println("");
            }
            System.out.println("Flow Files transferred to " + relationship.getName() + ": " + files.size() + "\n");
        }
    }
}
