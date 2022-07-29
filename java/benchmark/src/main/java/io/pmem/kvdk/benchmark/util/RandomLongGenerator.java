/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

package io.pmem.kvdk.benchmark.util;

import java.util.SplittableRandom;

public class RandomLongGenerator implements LongGenerator {
    private SplittableRandom random;
    private long low;
    private long high;

    public RandomLongGenerator(long low, long high) {
        this.low = low;
        this.high = high;
        this.random = new SplittableRandom();
    }

    @Override
    public long gen() {
        return random.nextLong(low, high);
    }
}
