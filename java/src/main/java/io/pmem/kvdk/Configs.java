/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

package io.pmem.kvdk;

public class Configs extends KVDKObject {
  static {
    Engine.loadLibrary();
  }

  public Configs() {
    super(newConfigs());
  }

  public Configs setPMemFileSize(final long size) {
    assert(isOwningHandle());
    setPMemFileSize(nativeHandle_, size);
    return this;
  }

  public Configs setPMemSegmentBlocks(final long blocks) {
    assert(isOwningHandle());
    setPMemSegmentBlocks(nativeHandle_, blocks);
    return this;
  }

  public Configs setHashBucketNum(final long num) {
    assert(isOwningHandle());
    setHashBucketNum(nativeHandle_, num);
    return this;
  }

  private static native long newConfigs();

  private native void setPMemFileSize(long handle, long size);
  private native void setPMemSegmentBlocks(long handle, long blocks);
  private native void setHashBucketNum(long handle, long num);

  @Override
  protected final native void closeInternal(long handle);
}
