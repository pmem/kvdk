/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

package io.pmem.kvdk;

/** Base calss for KVDK object that hold a reference to native C++ object. */
public abstract class KVDKObject extends AbstractNativeReference {
    /** An immutable reference to the value of the C++ pointer. */
    protected final long nativeHandle_;

    protected KVDKObject(final long nativeHandle) {
        super(true);
        this.nativeHandle_ = nativeHandle;
    }

    @Override
    protected void closeInternal() {
        closeInternal(nativeHandle_);
    }

    protected abstract void closeInternal(final long handle);

    public long getNativeHandle() {
        return nativeHandle_;
    }
}
