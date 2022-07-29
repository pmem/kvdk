/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

package io.pmem.kvdk.benchmark.util;

public class RangeLongGenerator implements LongGenerator {
    private long low;
    private long high;
    private long step;
    private long curr;

    public RangeLongGenerator(long low, long high, long step) {
        this.low = low;
        this.high = high;
        this.step = step;
        this.curr = this.low;
    }

    @Override
    public long gen() {
        long old = curr;
        curr += step;

        assert (old < high);
        return old;
    }
}
