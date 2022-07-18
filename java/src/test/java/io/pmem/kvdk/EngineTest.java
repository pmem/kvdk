/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

package io.pmem.kvdk;

import java.io.File;
import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class EngineTest {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void openKVDKEngine() throws KVDKException, IOException {
    File tempDir = folder.newFolder();
    String enginePath = tempDir.getAbsolutePath();

    Configs engineConfigs = new Configs();
    engineConfigs.setMaxAccessThreads(4);
    engineConfigs.setPMemSegmentBlocks(2L << 20);
    engineConfigs.setPMemFileSize(1L << 30); // 1 GB

    Engine kvdkEngine = Engine.open(enginePath, engineConfigs);

    kvdkEngine.close();
  }
}
