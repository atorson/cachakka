package com.cachakka.streaming.core.utils;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.io.File.separator;
import static java.util.stream.Collectors.toList;

/**
 * Abstraction for process that is running a JVM. It encapsulated the creation via it's build {@see JvmProcessBuilder}
 * and provides various helper methods to easily interact with them.
 * <p>
 * The purpose of this class is to allow easy integration into complex integration test scenarios for distributed
 * systems.
 *
 */
public class MultiJvmUtils {

    /**
     * Convenience method to create a new builder.
     *
     * @return the builder
     */
    public static JvmProcessBuilder builder() {
        return new JvmProcessBuilder();
    }


    /**
     * Builder for creating and starting a new {@see JvmProcess}.
     */
    public static final class JvmProcessBuilder {
        private List<String> classpathElements = new ArrayList<>();
        private List<String> arguments = new ArrayList<>();
        private List<String> jvmOptions = new ArrayList<>();
        private String mainClassName;
        private String fatJarPath;

        private void checkIfFatJar() {
            if (fatJarPath != null)
                throw new RuntimeException("Can't be used with fat-jar");
        }

        /**
         * Add single classpath entry
         *
         * @param classpathElement
         * @return
         */
        public JvmProcessBuilder addClasspathElement(String classpathElement) {
            checkIfFatJar();
            classpathElements.add(classpathElement);
            return this;
        }

        /**
         * Add a list of classpathentries.
         *
         * @param classpathElements a List of classpath entries
         * @return the builder
         */
        public JvmProcessBuilder addClasspathElements(Collection<String> classpathElements) {
            checkIfFatJar();
            classpathElements.addAll(classpathElements);
            return this;
        }

        /**
         * Add the classpath of the JVM spawning the new one. Only needed in soecial cases (e.g. testing this exact class).
         *
         * @return the builder
         */
        public JvmProcessBuilder addCurrentClasspath() {
            checkIfFatJar();
            classpathElements.add(System.getProperty("java.class.path"));
            return this;
        }

        /**
         * Set the main class to nbe executed when the JVM starts up. Only needed if no class has been provided in
         * the MANIFEST.MF (that's where fat-jars put that information)
         *
         * @param mainClassName fqn of the class to run
         * @return the builder
         */
        public JvmProcessBuilder mainClass(String mainClassName) {
            checkIfFatJar();
            this.mainClassName = mainClassName;
            return this;
        }

        /**
         * Add commandline arguments for the class to be run (e.g. "-myparam=666").
         *
         * @param argument the full argument
         * @return the builder
         */
        public JvmProcessBuilder addArgument(String argument) {
            this.arguments.add(argument);
            return this;
        }

        /**
         * Set JVM options, e.g. "-XX:-DisableExplicitGC"
         *
         * @param argument the full argument
         * @return the builder
         */
        public JvmProcessBuilder addJvmOption(String argument) {
            this.jvmOptions.add(argument);
            return this;
        }

        /**
         * Set the path of a fat-jar to be used.
         *
         * @param fatJarPath full path to the fat-jar
         */
        public void fatJarPath(String fatJarPath) {
            if (!classpathElements.isEmpty())
                throw new RuntimeException("fatJar can't be used together with classpath-entries!");
            this.fatJarPath = fatJarPath;
        }

        /**
         * Build and start the process.
         *
         * @return the new process
         */
        public JvmProcess build() {
            if (fatJarPath != null)
                return new JvmProcess(jvmOptions, arguments, fatJarPath).start();
            return new JvmProcess(jvmOptions, arguments, classpathElements, mainClassName).start();
        }
    }

    public static final class InputStreamProcessor {
        private InputStream inputStream;
        private List<Consumer<String>> consumerList = new CopyOnWriteArrayList<>();
        private Thread thread;
        private volatile boolean stop = false;

        public InputStreamProcessor(InputStream inputStream) {
            this.inputStream = inputStream;
            thread = new Thread() {
                @Override
                public void run() {
                    try {
                        try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
                            String line;
                            while (((line = br.readLine()) != null) && !stop) {
                                for (Consumer<String> c : consumerList) {
                                    c.accept(line);
                                }
                            }
                        }
                    } catch (IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                }
            };
        }

        public InputStreamProcessor addConsumer(Consumer<String> consumer) {
            consumerList.add(consumer);
            return this;
        }

        public InputStreamProcessor removeConsumer(Consumer<String> consumer) {
            consumerList.remove(consumer);
            return this;
        }

        public InputStreamProcessor start() {
            thread.start();
            return this;
        }

        public InputStreamProcessor stop() {
            stop = true;
            thread.interrupt();
            return this;
        }
    }

    public static final class JvmProcess {
        String path = System.getProperty("java.home")
                + separator + "bin" + separator + "java";
        Process process;
        ProcessBuilder processBuilder;
        InputStreamProcessor stdOut;
        InputStreamProcessor errorOut;

        /**
         * Constructor for a JVM where a specific class is to be run in.
         *
         * @param jvmOptions        -XX Options
         * @param arguments         applications arguments
         * @param classpathElements classpath entries to be provided in the new JVM
         * @param classToRun        the class to run when the JVM comes up
         */
        private JvmProcess(List<String> jvmOptions, List<String> arguments, List<String> classpathElements, String classToRun) {
            String classpath = classpathElements.stream().collect(Collectors.joining(System.getProperty("path.separator")));
            List<String> command = new ArrayList<>();
            command.add(path);
            command.addAll(jvmOptions);
            command.add("-cp");
            command.add(classpath);
            command.add(classToRun);
            command.addAll(arguments);
            processBuilder =
                    new ProcessBuilder(command);
        }

        /**
         * Constructor for a JvmProcess that uses a FatJar
         *
         * @param jvmOptions   -XX Options
         * @param arguments    applications arguments
         * @param pathToFatJar full path to the fatJar to be run
         */
        private JvmProcess(List<String> jvmOptions, List<String> arguments, String pathToFatJar) {
            List<String> command = new ArrayList<>();
            command.add(path);
            command.addAll(jvmOptions);
            command.add("-jar");
            command.add(pathToFatJar);
            command.addAll(arguments);
            processBuilder =
                    new ProcessBuilder(command);
        }

        /**
         * Start the process.
         *
         * @return fluent API
         */
        protected JvmProcess start() {
            try {
                process = processBuilder.start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return this;
        }

        /**
         * If called this method will block until the process has died.
         *
         * @return fluent API
         */
        public JvmProcess waitFor() {
            try {
                process.waitFor();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return this;
        }

        /**
         * Provide the exit code of the closed process.
         *
         * @return the exit code (0 = Success, everything else is a failure)
         */
        public int exitValue() {
            return process.exitValue();
        }

        /**
         * Destroy the process.
         */
        public void destroy() {
            try {
                if (stdOut != null)
                    stdOut.stop();
                if (errorOut != null)
                    errorOut.stop();
            } finally {
                process.destroyForcibly();
            }
        }

        /**
         * Collect each line of standard-output generated by the closed JVM in a List.
         * THIS METHOD BLOCKS UNTIL THE SPAWNED JVM EXITED
         *
         * @return list containing an entry for each line of JVM-output
         */
        public List<String> collectStandardOutput() {
            return collectFromInputStream(process.getInputStream());
        }

        /**
         * Collect each line of errorOut-output generated by the closed JVM in a List.
         * THIS METHOD BLOCKS UNTIL THE SPAWNED JVM EXITED
         *
         * @return list containing an entry for each line of JVM-output
         */
        public List<String> collectErrorOutput() {
            return collectFromInputStream(process.getErrorStream());
        }

        private List<String> collectFromInputStream(InputStream is) {
            try {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
                    return br.lines().collect(toList());
                }
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }

        /**
         * Returns the {@see InputStreamProcessor} that is associated with the standard-output of this process.
         *
         * @return the processor
         */
        public InputStreamProcessor stdOutProcessor() {
            if (stdOut == null)
                stdOut = new InputStreamProcessor(process.getInputStream());
            return stdOut;
        }

        /**
         * Returns the {@see InputStreamProcessor} that is associated with the errorOut-output of this process.
         *
         * @return the processor
         */
        public InputStreamProcessor errorProcessor() {
            if (stdOut == null)
                stdOut = new InputStreamProcessor(process.getErrorStream());
            return stdOut;
        }

    }
}

