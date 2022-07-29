/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

package io.pmem.kvdk;

import java.util.concurrent.atomic.AtomicBoolean;

/** Immutable reference to native C++ object. */
public abstract class AbstractNativeReference implements AutoCloseable {
    /** Indicating whether this reference is responsible to free the native C++ object. */
    protected final AtomicBoolean owningHandle_;

    /**
     * Constrctor.
     *
     * @param owningHandle
     */
    protected AbstractNativeReference(final boolean owningHandle) {
        this.owningHandle_ = new AtomicBoolean(owningHandle);
    }

    public boolean isOwningHandle() {
        return owningHandle_.get();
    }

    /** Release the responsibility of freeing native C++ object. */
    protected final void disOwnNativeHandle() {
        owningHandle_.set(false);
    }

    @Override
    public void close() {
        if (owningHandle_.compareAndSet(true, false)) {
            closeInternal();
        }
    }

    protected abstract void closeInternal();
}
