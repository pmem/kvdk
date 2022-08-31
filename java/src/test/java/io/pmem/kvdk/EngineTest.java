/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

package io.pmem.kvdk;

import static org.junit.Assert.assertEquals;

import io.pmem.kvdk.Status.Code;
import org.junit.Test;

public class EngineTest extends EngineTestBase {
    @Test
    public void testOpenAndCloseKVDKEngine() {}

    @Test
    public void testAnonymousCollection() throws KVDKException {
        String key = "key1";
        String value1 = "value1";

        // put
        kvdkEngine.put(key.getBytes(), value1.getBytes());
        assertEquals(value1, new String(kvdkEngine.get(key.getBytes())));

        // rewrite
        String value2 = "value2";
        kvdkEngine.put(key.getBytes(), value2.getBytes());
        assertEquals(value2, new String(kvdkEngine.get(key.getBytes())));

        // delete
        kvdkEngine.delete(key.getBytes());
        assertEquals(null, kvdkEngine.get(key.getBytes()));

        // delete nonexistent key: OK
        kvdkEngine.delete(key.getBytes());

        // put special characters
        String value3 = "value\0with_zero";
        kvdkEngine.put(key.getBytes(), value3.getBytes());
        assertEquals(value3, new String(kvdkEngine.get(key.getBytes())));
    }

    @Test
    public void testSortedCollection() throws KVDKException {
        String name = "collection\0\nname";
        NativeBytesHandle nameHandle = new NativeBytesHandle(name.getBytes());

        // create
        kvdkEngine.sortedCreate(nameHandle);

        // duplicate creation: Not OK
        try {
            kvdkEngine.sortedCreate(nameHandle);
        } catch (KVDKException ex) {
            // should be Existed
            assertEquals(ex.getStatus().getCode(), Code.Existed);
        }

        // destroy
        kvdkEngine.sortedDestroy(nameHandle);

        // create again: Ok
        kvdkEngine.sortedCreate(nameHandle);

        String key = "key\01";
        String value = "value\01";

        // put
        kvdkEngine.sortedPut(nameHandle, key.getBytes(), value.getBytes());

        // size
        assertEquals(1, kvdkEngine.sortedSize(nameHandle));

        // get
        assertEquals(value, new String(kvdkEngine.sortedGet(nameHandle, key.getBytes())));

        // delete
        kvdkEngine.sortedDelete(nameHandle, key.getBytes());
        assertEquals(null, kvdkEngine.sortedGet(nameHandle, key.getBytes()));

        // size
        assertEquals(0, kvdkEngine.sortedSize(nameHandle));

        // delete nonexistent key: OK
        kvdkEngine.sortedDelete(nameHandle, key.getBytes());

        // destroy
        kvdkEngine.sortedDestroy(nameHandle);

        // destroy destroyed sorted collection: OK
        kvdkEngine.sortedDestroy(nameHandle);

        // delete on destroyed sorted collection: OK
        kvdkEngine.sortedDelete(nameHandle, key.getBytes());

        // put on destroyed sorted collection: Not OK
        try {
            kvdkEngine.sortedPut(nameHandle, key.getBytes(), value.getBytes());
        } catch (KVDKException ex) {
            // should be NotFound
            assertEquals(ex.getStatus().getCode(), Code.NotFound);
        }

        // size on destroyed sorted collection: Not OK
        try {
            kvdkEngine.sortedSize(nameHandle);
        } catch (KVDKException ex) {
            // should be NotFound
            assertEquals(ex.getStatus().getCode(), Code.NotFound);
        }

        // close name handle
        nameHandle.close();
    }

    @Test
    public void testExpiration() throws KVDKException, InterruptedException {
        String key1 = "key1";
        String value1 = "value1";

        // put
        kvdkEngine.put(key1.getBytes(), value1.getBytes());
        assertEquals(value1, new String(kvdkEngine.get(key1.getBytes())));

        // expire
        kvdkEngine.expire(key1.getBytes(), 199);
        Thread.sleep(200);

        // check
        assertEquals(null, kvdkEngine.get(key1.getBytes()));
    }

    @Test
    public void testExpirationWithWriteOptions() throws KVDKException, InterruptedException {
        String key1 = "key1";
        String value1 = "value1";

        WriteOptions writeOptions = new WriteOptions(199, true);

        // put
        kvdkEngine.put(key1.getBytes(), value1.getBytes(), writeOptions);
        assertEquals(value1, new String(kvdkEngine.get(key1.getBytes())));

        Thread.sleep(200);

        // check
        assertEquals(null, kvdkEngine.get(key1.getBytes()));
    }
}
