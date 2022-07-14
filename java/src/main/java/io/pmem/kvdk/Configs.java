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

  private static native long newConfigs();

  @Override
  protected final native void closeInternal(long handle);
}
