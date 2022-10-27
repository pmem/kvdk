/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

package io.pmem.kvdk;

import org.junit.Test;

public class ConfigsTest {
    @Test
    public void testCreateConfigs() {
        Configs configs = new Configs();

        configs.setMaxAccessThreads(8);

        configs.close();
    }
}
