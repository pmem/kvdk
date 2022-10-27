/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

package io.pmem.kvdk;

/** Options of expiration */
public class WriteOptions {
    private long ttlInMillis;
    private boolean updateTtlIfExisted;

    public WriteOptions() {
        this(Long.MAX_VALUE, true);
    }

    public WriteOptions(long ttlInMillis, boolean updateTtlIfExisted) {
        this.ttlInMillis = ttlInMillis;
        this.updateTtlIfExisted = updateTtlIfExisted;
    }

    public long getTtlInMillis() {
        return ttlInMillis;
    }

    public boolean isUpdateTtlIfExisted() {
        return updateTtlIfExisted;
    }
}
