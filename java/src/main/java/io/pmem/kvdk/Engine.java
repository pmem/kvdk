/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

package io.pmem.kvdk;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class Engine extends KVDKObject {
  static {
    Engine.loadLibrary();
  }

  protected Engine(final long nativeHandle) {
    super(nativeHandle);
  }

  public static Engine open(final String path, final Configs configs)
      throws KVDKException {
    final Engine engine = new Engine(open(path, configs.getNativeHandle()));
    return engine;
  }

  @Override
  protected final native void closeInternal(long handle);
  private native static long open(final String path,
      final long cfg_handle) throws KVDKException;

  private enum LibraryState {
    NOT_LOADED,
    LOADING,
    LOADED
  }

  private static final AtomicReference<LibraryState> libraryLoaded =
      new AtomicReference<>(LibraryState.NOT_LOADED);

  /**
   * Loads the necessary library files.
   * Calling this method twice will have no effect.
   * By default the method extracts the shared library for loading at
   * java.io.tmpdir, however, you can override this temporary location by
   * setting the environment variable KVDK_SHARED_LIB_DIR.
   */
  public static void loadLibrary() {
    if (libraryLoaded.get() == LibraryState.LOADED) {
      return;
    }

    if (libraryLoaded.compareAndSet(LibraryState.NOT_LOADED,
        LibraryState.LOADING)) {
      final String tmpDir = System.getenv("KVDK_SHARED_LIB_DIR");
      
      try {
        NativeLibraryLoader.getInstance().loadLibrary(tmpDir);
      } catch (final IOException e) {
        libraryLoaded.set(LibraryState.NOT_LOADED);
        throw new RuntimeException("Unable to load the KVDK shared library",
            e);
      }

      libraryLoaded.set(LibraryState.LOADED);
      return;
    }

    while (libraryLoaded.get() == LibraryState.LOADING) {
      try {
        Thread.sleep(10);
      } catch(final InterruptedException e) {
        //ignore
      }
    }
  }

  /**
   * Tries to load the necessary library files from the given list of
   * directories.
   *
   * @param paths a list of strings where each describes a directory
   *     of a library.
   */
  public static void loadLibrary(final List<String> paths) {
    if (libraryLoaded.get() == LibraryState.LOADED) {
      return;
    }

    if (libraryLoaded.compareAndSet(LibraryState.NOT_LOADED,
        LibraryState.LOADING)) {
      boolean success = false;
      UnsatisfiedLinkError err = null;
      for (final String path : paths) {
        try {
          System.load(path + "/" + "libkvdkjni.so");
          success = true;
          break;
        } catch (final UnsatisfiedLinkError e) {
          err = e;
        }
      }
      if (!success) {
        libraryLoaded.set(LibraryState.NOT_LOADED);
        throw err;
      }

      libraryLoaded.set(LibraryState.LOADED);
      return;
    }

    while (libraryLoaded.get() == LibraryState.LOADING) {
      try {
        Thread.sleep(10);
      } catch(final InterruptedException e) {
        //ignore
      }
    }
  }

}
