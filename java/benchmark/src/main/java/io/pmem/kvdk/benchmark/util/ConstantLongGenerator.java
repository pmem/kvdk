/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

package io.pmem.kvdk.benchmark.util;

public class ConstantLongGenerator implements LongGenerator {
    private long value;

    public ConstantLongGenerator(long value) {
        this.value = value;
    }

    @Override
    public long gen() {
        return value;
    }
}
