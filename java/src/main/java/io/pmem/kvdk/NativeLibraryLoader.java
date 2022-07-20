/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

package io.pmem.kvdk;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * This class is used to load the KVDK shared library from system or jar. When loading from jar, the
 * shared library is extracted to a temp folder and loaded from there.
 */
public class NativeLibraryLoader {
    private static final NativeLibraryLoader instance = new NativeLibraryLoader();
    private static boolean initialized = false;

    private static final String jniLibraryName = "kvdkjni";
    private static final String jniLibraryFileName = System.mapLibraryName("kvdkjni");
    private static final String atomicLibraryFileName = "libatomic.so.1";
    private static final String tempAtomicLibraryFilePrefix = "libatomic";
    private static final String tempAtomicLibraryFileSuffix = ".so.1";
    private static final String tempJniLibraryFilePrefix = "libkvdkjni";
    private static final String tempJniLibraryFileSuffix = ".so";

    public static NativeLibraryLoader getInstance() {
        return instance;
    }

    public synchronized void loadLibrary(final String tmpDir) throws IOException {
        try {
            // try system dynamic library
            System.loadLibrary(jniLibraryName);
            return;
        } catch (final UnsatisfiedLinkError ule) {
            // ignore - then try from jar
        }

        // try jar
        loadLibraryFromJar(tmpDir);
    }

    void loadLibraryFromJar(final String tmpDir) throws IOException {
        if (!initialized) {
            System.load(
                    loadLibraryFromJarToTemp(
                                    tmpDir,
                                    atomicLibraryFileName,
                                    tempAtomicLibraryFilePrefix,
                                    tempAtomicLibraryFileSuffix)
                            .getAbsolutePath());
            System.load(
                    loadLibraryFromJarToTemp(
                                    tmpDir,
                                    jniLibraryFileName,
                                    tempJniLibraryFilePrefix,
                                    tempJniLibraryFileSuffix)
                            .getAbsolutePath());
            initialized = true;
        }
    }

    File loadLibraryFromJarToTemp(
            final String tmpDir,
            String libraryFileName,
            String tempFilePrefix,
            String tempFileSuffix)
            throws IOException {
        InputStream is = null;
        try {
            // attempt to look up the static library in the jar file
            is = getClass().getClassLoader().getResourceAsStream(libraryFileName);

            if (is == null) {
                throw new RuntimeException(libraryFileName + " was not found inside JAR.");
            }

            // create a temporary file to copy the library to
            final File temp;
            if (tmpDir == null || tmpDir.isEmpty()) {
                temp = File.createTempFile(tempFilePrefix, tempFileSuffix);
            } else {
                final File parentDir = new File(tmpDir);
                if (!parentDir.exists()) {
                    throw new RuntimeException(
                            "Directory: " + parentDir.getAbsolutePath() + " does not exist!");
                }
                temp = new File(parentDir, libraryFileName);
                if (temp.exists() && !temp.delete()) {
                    throw new RuntimeException(
                            "File: "
                                    + temp.getAbsolutePath()
                                    + " already exists and cannot be removed.");
                }
                if (!temp.createNewFile()) {
                    throw new RuntimeException(
                            "File: " + temp.getAbsolutePath() + " could not be created.");
                }
            }
            if (!temp.exists()) {
                throw new RuntimeException("File " + temp.getAbsolutePath() + " does not exist.");
            } else {
                temp.deleteOnExit();
            }

            // copy the library from the Jar file to the temp destination
            Files.copy(is, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);

            // return the temporary library file
            return temp;

        } finally {
            if (is != null) {
                is.close();
            }
        }
    }

    /** Private constructor to disallow instantiation */
    private NativeLibraryLoader() {}
}
