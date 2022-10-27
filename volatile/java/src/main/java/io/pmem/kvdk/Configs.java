/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

package io.pmem.kvdk;

/** KVDK engine configs. */
public class Configs extends KVDKObject {
    static {
        Engine.loadLibrary();
    }

    public Configs() {
        super(newConfigs());
    }

    public Configs setMaxAccessThreads(final long num_threads) {
        setMaxAccessThreads(nativeHandle_, num_threads);
        return this;
    }

    public Configs setHashBucketNum(final long num) {
        setHashBucketNum(nativeHandle_, num);
        return this;
    }

    public Configs setDestMemoryNodes(final String nodes) {
        setDestMemoryNodes(nativeHandle_, nodes);
        return this;
    }

    public Configs setOptLargeSortedCollectionRecovery(final boolean opt) {
        setOptLargeSortedCollectionRecovery(nativeHandle_, opt);
        return this;
    }

    public Configs setUseDevDaxMode(final boolean use) {
        setUseDevDaxMode(nativeHandle_, use);
        return this;
    }

    public Configs setCleanThreads(final long num_threads) {
        setCleanThreads(nativeHandle_, num_threads);
        return this;
    }

    // Native methods
    private static native long newConfigs();

    private native void setMaxAccessThreads(long handle, long num);

    private native void setHashBucketNum(long handle, long num);

    private native void setDestMemoryNodes(long handle, String nodes);

    private native void setOptLargeSortedCollectionRecovery(long handle, boolean opt);

    private native void setUseDevDaxMode(long handle, boolean use);

    private native void setCleanThreads(long handle, long num);

    @Override
    protected final native void closeInternal(long handle);
}
