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

    public Iterator(final long iteratorHandle, final long engineHandle) {
        super(iteratorHandle);
        this.engineHandle = engineHandle;
    }

    @Override
    protected void closeInternal(long handle) {
        closeInternal(handle, engineHandle);
    }

    // Native methods
    protected native void closeInternal(long iteratorHandle, long engineHandle);
}
