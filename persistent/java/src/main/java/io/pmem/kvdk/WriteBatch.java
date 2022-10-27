/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

package io.pmem.kvdk;

/** A WriteBatch can be used to prepare multiple operations before a commit. */
public class WriteBatch extends KVDKObject {
    static {
        Engine.loadLibrary();
    }

    protected WriteBatch(final long handle) {
        super(handle);
    }

    public void stringPut(final byte[] key, final byte[] value) {
        stringPut(nativeHandle_, key, 0, key.length, value, 0, value.length);
    }

    public void stringPut(
            final byte[] key,
            int keyOffset,
            int keyLength,
            final byte[] value,
            int valueOffset,
            int valueLength) {
        stringPut(nativeHandle_, key, keyOffset, keyLength, value, valueOffset, valueLength);
    }

    public void stringDelete(final byte[] key) {
        stringDelete(nativeHandle_, key, 0, key.length);
    }

    public void stringDelete(final byte[] key, int keyOffset, int keyLength) {
        stringDelete(nativeHandle_, key, keyOffset, keyLength);
    }

    public void sortedPut(
            final NativeBytesHandle nameHandle, final byte[] key, final byte[] value) {
        sortedPut(
                nativeHandle_,
                nameHandle.getNativeHandle(),
                nameHandle.getLength(),
                key,
                0,
                key.length,
                value,
                0,
                value.length);
    }

    public void sortedPut(
            final NativeBytesHandle nameHandle,
            final byte[] key,
            int keyOffset,
            int keyLength,
            final byte[] value,
            int valueOffset,
            int valueLength) {
        sortedPut(
                nativeHandle_,
                nameHandle.getNativeHandle(),
                nameHandle.getLength(),
                key,
                keyOffset,
                keyLength,
                value,
                valueOffset,
                valueLength);
    }

    public void sortedDelete(final NativeBytesHandle nameHandle, final byte[] key) {
        sortedDelete(
                nativeHandle_,
                nameHandle.getNativeHandle(),
                nameHandle.getLength(),
                key,
                0,
                key.length);
    }

    public void sortedDelete(
            final NativeBytesHandle nameHandle, final byte[] key, int keyOffset, int keyLength) {
        sortedDelete(
                nativeHandle_,
                nameHandle.getNativeHandle(),
                nameHandle.getLength(),
                key,
                keyOffset,
                keyLength);
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

    private native void stringPut(
            long handle,
            byte[] key,
            int keyOffset,
            int keyLength,
            byte[] value,
            int valueOffset,
            int valueLength);

    private native void stringDelete(long handle, byte[] key, int keyOffset, int keyLength);

    private native void sortedPut(
            long engineHandle,
            long nameHandle,
            int nameLenth,
            byte[] key,
            int keyOffset,
            int keyLength,
            byte[] value,
            int valueOffset,
            int valueLength);

    private native void sortedDelete(
            long engineHandle,
            long nameHandle,
            int nameLenth,
            byte[] key,
            int keyOffset,
            int keyLength);

    private native void clear(long handle);

    private native long size(long handle);
}
