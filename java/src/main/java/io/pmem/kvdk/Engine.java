/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

package io.pmem.kvdk;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/** The KVDK engine, providing the APIs for Key-Value operations. */
public class Engine extends KVDKObject {
    private static final String atomicLibraryFileName = "libatomic.so.1";
    private static final String stdLibraryFileName = "libstdc++.so.6";
    private static final String jniLibraryFileName = System.mapLibraryName("kvdkjni");

    /**
     * Current state of loading native library. This must be defined before calling loading
     * loadLibrary().
     */
    private static final AtomicReference<LibraryState> libraryLoaded =
            new AtomicReference<>(LibraryState.NOT_LOADED);

    static {
        Engine.loadLibrary();
    }

    protected Engine(final long nativeHandle) {
        super(nativeHandle);
    }

    /**
     * Open a KVDK engine instance.
     *
     * @param path A directory to PMem
     * @param configs KVDK engine configs
     * @return
     * @throws KVDKException
     */
    public static Engine open(final String path, final Configs configs) throws KVDKException {
        final Engine engine = new Engine(open(path, configs.getNativeHandle()));
        return engine;
    }

    public void put(final byte[] key, final byte[] value) throws KVDKException {
        put(nativeHandle_, key, value);
    }

    /**
     * @param key
     * @return Value as byte array, null if the specified key doesn't exist.
     * @throws KVDKException
     */
    public byte[] get(final byte[] key) throws KVDKException {
        return get(nativeHandle_, key);
    }

    public void delete(final byte[] key) throws KVDKException {
        delete(nativeHandle_, key);
    }

    public void sortedCreate(final NativeBytesHandle nameHandle) throws KVDKException {
        sortedCreate(nativeHandle_, nameHandle.getNativeHandle(), nameHandle.getLength());
    }

    public void sortedDestroy(final NativeBytesHandle nameHandle) throws KVDKException {
        sortedDestroy(nativeHandle_, nameHandle.getNativeHandle(), nameHandle.getLength());
    }

    public long sortedSize(final NativeBytesHandle nameHandle) throws KVDKException {
        return sortedSize(nativeHandle_, nameHandle.getNativeHandle(), nameHandle.getLength());
    }

    public void sortedPut(final NativeBytesHandle nameHandle, final byte[] key, final byte[] value)
            throws KVDKException {
        sortedPut(nativeHandle_, nameHandle.getNativeHandle(), nameHandle.getLength(), key, value);
    }

    /**
     * @param nameHandle
     * @param key
     * @return Value as byte array, null if the specified key doesn't exist.
     * @throws KVDKException
     */
    public byte[] sortedGet(final NativeBytesHandle nameHandle, final byte[] key)
            throws KVDKException {
        return sortedGet(nativeHandle_, nameHandle.getNativeHandle(), nameHandle.getLength(), key);
    }

    public void sortedDelete(final NativeBytesHandle nameHandle, final byte[] key)
            throws KVDKException {
        sortedDelete(nativeHandle_, nameHandle.getNativeHandle(), nameHandle.getLength(), key);
    }

    public Iterator newSortedIterator(final NativeBytesHandle nameHandle) throws KVDKException {
        long iteratorHandle =
                newSortedIterator(
                        nativeHandle_, nameHandle.getNativeHandle(), nameHandle.getLength());

        return new Iterator(iteratorHandle, nativeHandle_);
    }

    public void releaseSortedIterator(final Iterator iterator) throws KVDKException {
        iterator.close();
    }

    // Native methods
    @Override
    protected final native void closeInternal(long handle);

    private static native long open(final String path, final long cfg_handle) throws KVDKException;

    private native void put(long handle, byte[] key, byte[] value);

    private native byte[] get(long handle, byte[] key);

    private native void delete(long handle, byte[] key);

    private native void sortedCreate(long engineHandle, long nameHandle, int nameLenth);

    private native void sortedDestroy(long engineHandle, long nameHandle, int nameLenth);

    private native long sortedSize(long engineHandle, long nameHandle, int nameLenth);

    private native void sortedPut(
            long engineHandle, long nameHandle, int nameLenth, byte[] key, byte[] value);

    private native byte[] sortedGet(long engineHandle, long nameHandle, int nameLenth, byte[] key);

    private native void sortedDelete(long engineHandle, long nameHandle, int nameLenth, byte[] key);

    private native long newSortedIterator(long engineHandle, long nameHandle, int nameLenth);

    private enum LibraryState {
        NOT_LOADED,
        LOADING,
        LOADED
    }

    /**
     * Loads the necessary library files. Calling this method twice will have no effect. By default
     * the method extracts the shared library for loading at java.io.tmpdir, however, you can
     * override this temporary location by setting the environment variable KVDK_SHARED_LIB_DIR.
     */
    public static void loadLibrary() {
        if (libraryLoaded.get() == LibraryState.LOADED) {
            return;
        }

        if (libraryLoaded.compareAndSet(LibraryState.NOT_LOADED, LibraryState.LOADING)) {
            final String tmpDir = System.getenv("KVDK_SHARED_LIB_DIR");

            try {
                NativeLibraryLoader.getInstance().loadLibrary(tmpDir);
            } catch (final IOException e) {
                libraryLoaded.set(LibraryState.NOT_LOADED);
                throw new RuntimeException("Unable to load the KVDK shared library", e);
            }

            libraryLoaded.set(LibraryState.LOADED);
            return;
        }

        while (libraryLoaded.get() == LibraryState.LOADING) {
            try {
                Thread.sleep(10);
            } catch (final InterruptedException e) {
                // ignore
            }
        }
    }

    /**
     * Tries to load the necessary library files from the given list of directories.
     *
     * @param paths a list of strings where each describes a directory of a library.
     */
    public static void loadLibrary(final List<String> paths) {
        if (libraryLoaded.get() == LibraryState.LOADED) {
            return;
        }

        if (libraryLoaded.compareAndSet(LibraryState.NOT_LOADED, LibraryState.LOADING)) {
            boolean success = false;
            UnsatisfiedLinkError err = null;
            for (final String path : paths) {
                try {
                    System.load(path + "/" + atomicLibraryFileName);
                    System.load(path + "/" + stdLibraryFileName);
                    System.load(path + "/" + jniLibraryFileName);
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
            } catch (final InterruptedException e) {
                // ignore
            }
        }
    }
}
