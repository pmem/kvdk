/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

package io.pmem.kvdk;

/**
 * This class is used to handle reference to native string view. Holding native string view avoids
 * repetitive coversion of bytes from Java to C++.
 */
public class NativeBytesHandle extends KVDKObject {
    static {
        Engine.loadLibrary();
    }

    private final int length;
    private final byte[] bytes;

    public NativeBytesHandle(final byte[] bytes) {
        super(newNativeBytes(bytes));
        this.length = bytes.length;
        this.bytes = bytes;
    }

    public int getLength() {
        return length;
    }

    public byte[] getBytes() {
        return bytes;
    }

    // Native methods
    private static native long newNativeBytes(byte[] bytes);

    @Override
    protected native void closeInternal(long handle);
}
