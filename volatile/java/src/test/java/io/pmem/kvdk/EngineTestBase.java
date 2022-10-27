/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

package io.pmem.kvdk;

import java.io.File;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class EngineTestBase {
    protected Engine kvdkEngine;

    @Rule public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void init() throws KVDKException, IOException {
        File tempDir = folder.newFolder();
        String enginePath = tempDir.getAbsolutePath();

        Configs engineConfigs = new Configs();
        engineConfigs.setHashBucketNum(1L << 10);
        engineConfigs.setMaxAccessThreads(4);

        kvdkEngine = Engine.open(enginePath, engineConfigs);
        engineConfigs.close();
    }

    @After
    public void teardown() {
        kvdkEngine.close();
    }
}
