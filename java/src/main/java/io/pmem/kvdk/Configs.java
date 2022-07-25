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
        assert (isOwningHandle());
        setMaxAccessThreads(nativeHandle_, num_threads);
        return this;
    }

    /**
     * @param size PMem file size should be larger than: pmem_segment_blocks * pmem_block_size *
     *     max_access_threads
     * @return
     */
    public Configs setPMemFileSize(final long size) {
        assert (isOwningHandle());
        setPMemFileSize(nativeHandle_, size);
        return this;
    }

    public Configs setPMemSegmentBlocks(final long blocks) {
        assert (isOwningHandle());
        setPMemSegmentBlocks(nativeHandle_, blocks);
        return this;
    }

    public Configs setHashBucketNum(final long num) {
        assert (isOwningHandle());
        setHashBucketNum(nativeHandle_, num);
        return this;
    }

    public Configs setPopulatePMemSpace(final boolean populate) {
        assert (isOwningHandle());
        setPopulatePMemSpace(nativeHandle_, populate);
        return this;
    }

    public Configs setOptLargeSortedCollectionRecovery(final boolean opt) {
        assert (isOwningHandle());
        setOptLargeSortedCollectionRecovery(nativeHandle_, opt);
        return this;
    }

    public Configs setUseDevDaxMode(final boolean use) {
        assert (isOwningHandle());
        setUseDevDaxMode(nativeHandle_, use);
        return this;
    }

    public Configs setCleanThreads(final long num_threads) {
        assert (isOwningHandle());
        setCleanThreads(nativeHandle_, num_threads);
        return this;
    }

    // Native methods
    private static native long newConfigs();

    private native void setMaxAccessThreads(long handle, long num);

    private native void setPMemFileSize(long handle, long size);

    private native void setPMemSegmentBlocks(long handle, long blocks);

    private native void setHashBucketNum(long handle, long num);

    private native void setPopulatePMemSpace(long handle, boolean populate);

    private native void setOptLargeSortedCollectionRecovery(long handle, boolean opt);

    private native void setUseDevDaxMode(long handle, boolean use);

    private native void setCleanThreads(long handle, long num);

    @Override
    protected final native void closeInternal(long handle);
}
