/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

package io.pmem.kvdk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class WriteBatchTest extends EngineTestBase {
    @Test
    public void testWriteBatchString() throws KVDKException {
        String key1 = "key1";
        String key2 = "key2";
        String value1 = "value1";
        String value2 = "value2";

        WriteBatch batch = kvdkEngine.writeBatchCreate();
        batch.stringPut(key1.getBytes(), value1.getBytes());
        batch.stringPut(key1.getBytes(), value2.getBytes());
        batch.stringPut(key2.getBytes(), value2.getBytes());
        batch.stringDelete(key2.getBytes());

        // If the batch is successfully written, there should be only key1-value2 in
        // anonymous global collection.
        kvdkEngine.batchWrite(batch);
        assertEquals(value2, new String(kvdkEngine.get(key1.getBytes())));
        assertNull(kvdkEngine.get(key2.getBytes()));
    }

    @Test
    public void testWriteBatchSorted() throws KVDKException {
        String name = "collection_name";
        String key1 = "key1";
        String key2 = "key2";
        String value1 = "value1";
        String value2 = "value2";

        NativeBytesHandle nameHandle = new NativeBytesHandle(name.getBytes());

        kvdkEngine.sortedCreate(nameHandle);

        WriteBatch batch = kvdkEngine.writeBatchCreate();
        batch.sortedPut(nameHandle, key1.getBytes(), value1.getBytes());
        batch.sortedPut(nameHandle, key1.getBytes(), value2.getBytes());
        batch.sortedPut(nameHandle, key2.getBytes(), value2.getBytes());
        batch.sortedDelete(nameHandle, key2.getBytes());

        // If the batch is successfully written, there should be only key1-value2 in
        // the sorted collection.
        kvdkEngine.batchWrite(batch);
        assertEquals(1, kvdkEngine.sortedSize(nameHandle));
        assertEquals(value2, new String(kvdkEngine.sortedGet(nameHandle, key1.getBytes())));
        assertNull(kvdkEngine.sortedGet(nameHandle, key2.getBytes()));

        batch.close();
        nameHandle.close();
    }
}
