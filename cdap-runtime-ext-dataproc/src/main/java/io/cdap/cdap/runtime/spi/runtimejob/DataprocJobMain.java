/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.runtime.spi.runtimejob;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.twill.internal.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Main class that will be called from dataproc driver.
 */
public class DataprocJobMain {
  private static final Logger LOG = LoggerFactory.getLogger(DataprocJobMain.class);
  private static final String ARCHIVE_FILES_JSON = "archive_files.json";
  private static final Gson GSON = new Gson();
  private static final Type LIST_TYPE = new TypeToken<List<String>>() { }.getType();

  /**
   * Main method to setup classpath and call the RuntimeJob.run() method.
   *
   * @param args the name of implementation of RuntimeJob class
   * @throws Exception any exception while running the job
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      throw new RuntimeException("An implementation of RuntimeJob classname and spark compat should be provided as " +
                                   "job arguments.");
    }

    String runtimeJobClassName = args[0];
    String sparkCompat = args[1];

    ClassLoader cl = DataprocJobMain.class.getClassLoader();
    if (!(cl instanceof URLClassLoader)) {
      throw new RuntimeException("Classloader is expected to be an instance of URLClassLoader");
    }

    // expand archive jars. This is needed because of CDAP-16456
    unArchiveJars();

    // create classpath from resources, application and twill jars
    URL[] urls = getClasspath((URLClassLoader) cl, ImmutableList.of(Constants.Files.RESOURCES_JAR,
                                                                    Constants.Files.APPLICATION_JAR,
                                                                    Constants.Files.TWILL_JAR));


    // create new URL classloader with provided classpath
    try (URLClassLoader newCL = new URLClassLoader(urls, cl.getParent())) {
      Thread.currentThread().setContextClassLoader(newCL);

      // load environment class and create instance of it
      String dataprocEnvClassName = DataprocRuntimeEnvironment.class.getName();
      Class<?> dataprocEnvClass = newCL.loadClass(dataprocEnvClassName);
      Object newDataprocEnvInstance = dataprocEnvClass.newInstance();

      try {
        // call initialize() method on dataprocEnvClass
        Method initializeMethod = dataprocEnvClass.getMethod("initialize", String.class);
        LOG.info("Invoking initialize() on {} with {}", dataprocEnvClassName, sparkCompat);
        initializeMethod.invoke(newDataprocEnvInstance, sparkCompat);

        // call run() method on runtimeJobClass
        Class<?> runEnvCls = newCL.loadClass(RuntimeJobEnvironment.class.getName());
        Class<?> runner = newCL.loadClass(runtimeJobClassName);
        Method method = runner.getMethod("run", runEnvCls);
        LOG.info("Invoking run() on {}", runtimeJobClassName);
        method.invoke(runner.newInstance(), newDataprocEnvInstance);

      } finally {
        // call destroy() method on envProviderClass
        Method closeMethod = dataprocEnvClass.getMethod("destroy");
        LOG.info("Invoking destroy() on {}", runtimeJobClassName);
        closeMethod.invoke(newDataprocEnvInstance);
      }

      LOG.info("Runtime job completed.");
    }
  }

  private static void unArchiveJars() {
    try (Reader reader = new BufferedReader(new FileReader(new File(ARCHIVE_FILES_JSON)))) {
      List<String> files = GSON.fromJson(reader, LIST_TYPE);
      for (String fileName : files) {
        unpack(fileName, fileName);
      }
    } catch (Exception e) {
      throw new IllegalArgumentException(
        String.format("Unable to read %s file. It should be a valid json containing " +
                        "list of archive files", ARCHIVE_FILES_JSON), e);
    }
  }

  /**
   * This method will generate class path by adding following to urls to front of default classpath:
   *
   * expanded.resource.jar
   * expanded.application.jar
   * expanded.application.jar/lib/*.jar
   * expanded.application.jar/classes
   * expanded.twill.jar
   * expanded.twill.jar/lib/*.jar
   * expanded.twill.jar/classes
   *
   */
  private static URL[] getClasspath(URLClassLoader cl, List<String> jarFiles) throws IOException {
    URL[] urls = cl.getURLs();
    List<URL> urlList = new ArrayList<>();
    for (String file : jarFiles) {
      File jarDir = new File(file);
      // add url for dir
      urlList.add(jarDir.toURI().toURL());
      if (file.equals(Constants.Files.RESOURCES_JAR)) {
        continue;
      }
      urlList.addAll(createClassPathURLs(jarDir));
    }

    urlList.addAll(Arrays.asList(urls));

    LOG.info("Classpath URLs: {}", urlList);
    return urlList.toArray(new URL[0]);
  }

  private static List<URL> createClassPathURLs(File dir) throws MalformedURLException {
    List<URL> urls = new ArrayList<>();
    // add jar urls from lib under dir
    addJarURLs(new File(dir, "lib"), urls);
    // add classes under dir
    urls.add(new File(dir, "classes").toURI().toURL());
    return urls;
  }

  private static void addJarURLs(File dir, List<URL> result) throws MalformedURLException {
    if (dir.listFiles() == null) {
      return;
    }
    for (File file : dir.listFiles()) {
      if (file.getName().endsWith(".jar")) {
        result.add(file.toURI().toURL());
      }
    }
  }

  private static void unpack(String archiveFileName, String targetDirName) throws IOException {
    String extension = getFileExtension(archiveFileName);
    switch (extension) {
      case "zip":
      case "jar":
        unJar(archiveFileName, targetDirName);
        break;
      default:
        throw new IllegalArgumentException(String.format("Unsupported compression type '%s'. Only 'zip' and 'jar'" +
                                                           " are supported.",
                                                         extension));
    }
  }

  private static void unJar(String archiveFileName, String targetDirName) throws IOException {
    File archive = new File(archiveFileName);
    File targetDirectory = new File(targetDirName + ".tmp");

    try (ZipInputStream zipIn = new ZipInputStream(new BufferedInputStream(new FileInputStream(archive)))) {
      Path targetPath = targetDirectory.toPath();
      Files.createDirectories(targetPath);

      ZipEntry entry;
      while ((entry = zipIn.getNextEntry()) != null) {
        Path output = targetPath.resolve(entry.getName());

        if (entry.isDirectory()) {
          Files.createDirectories(output);
        } else {
          Files.createDirectories(output.getParent());
          Files.copy(zipIn, output);
        }
      }
    } finally {
      archive.delete();
      targetDirectory.renameTo(new File(archiveFileName));
    }
  }

  private static String getFileExtension(String archiveFileName) {
    int dotIndex = archiveFileName.lastIndexOf('.');
    return archiveFileName.substring(dotIndex + 1);
  }
}
