/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

package io.pmem.kvdk;

/** Iterator to Key-Values in KVDK. */
public class Iterator extends KVDKObject {
    static {
        Engine.loadLibrary();
    }

    private final long engineHandle;

    protected Iterator(final long iteratorHandle, final long engineHandle) {
        super(iteratorHandle);
        this.engineHandle = engineHandle;
    }

    @Override
    protected void closeInternal(long handle) {
        closeInternal(handle, engineHandle);
    }

    public void seek(final byte[] key) {
        seek(nativeHandle_, key, 0, key.length);
    }

    public void seek(final byte[] key, final int keyOffset, int keyLength) {
        seek(nativeHandle_, key, keyOffset, keyLength);
    }

    public void seekToFirst() {
        seekToFirst(nativeHandle_);
    }

    public void seekToLast() {
        seekToLast(nativeHandle_);
    }

    public boolean isValid() {
        return isValid(nativeHandle_);
    }

    public void next() {
        next(nativeHandle_);
    }

    public void prev() {
        prev(nativeHandle_);
    }

    public byte[] key() {
        return key(nativeHandle_);
    }

    public byte[] value() {
        return value(nativeHandle_);
    }

    // Native methods
    protected native void closeInternal(long iteratorHandle, long engineHandle);

    private native void seek(long handle, byte[] key, int keyOffset, int keyLength);

    private native void seekToFirst(long handle);

    private native void seekToLast(long handle);

    private native boolean isValid(long handle);

    private native void next(long handle);

    private native void prev(long handle);

    private native byte[] key(long handle);

    private native byte[] value(long handle);
}
