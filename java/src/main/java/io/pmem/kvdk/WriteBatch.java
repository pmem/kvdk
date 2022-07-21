/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

package io.pmem.kvdk;

/** A WriteBatch can be used to prepare multiple operations before a commit. */
public class WriteBatch extends KVDKObject {
    static {
        Engine.loadLibrary();
    }

    public WriteBatch(final long handle) {
        super(handle);
    }

    public void stringPut(final byte[] key, final byte[] value) {
        stringPut(nativeHandle_, key, value);
    }

    public void stringDelete(final byte[] key) {
        stringDelete(nativeHandle_, key);
    }

    public void sortedPut(
            final NativeBytesHandle nameHandle, final byte[] key, final byte[] value) {
        sortedPut(nativeHandle_, nameHandle.getNativeHandle(), nameHandle.getLength(), key, value);
    }

    public void sortedDelete(final NativeBytesHandle nameHandle, final byte[] key) {
        sortedDelete(nativeHandle_, nameHandle.getNativeHandle(), nameHandle.getLength(), key);
    }

    public void clear() {
        clear(nativeHandle_);
    }

    public long size() {
        return size(nativeHandle_);
    }

    // Native methods
    @Override
    protected final native void closeInternal(long handle);

    private native void stringPut(long handle, byte[] key, byte[] value);

    private native void stringDelete(long handle, byte[] key);

    private native void sortedPut(
            long engineHandle, long nameHandle, int nameLenth, byte[] key, byte[] value);

    private native void sortedDelete(long engineHandle, long nameHandle, int nameLenth, byte[] key);

    private native void clear(long handle);

    private native long size(long handle);
}
