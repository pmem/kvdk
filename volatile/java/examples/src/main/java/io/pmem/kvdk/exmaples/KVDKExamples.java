/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

package io.pmem.kvdk.examples;

import io.pmem.kvdk.Configs;
import io.pmem.kvdk.Engine;
import io.pmem.kvdk.Iterator;
import io.pmem.kvdk.KVDKException;
import io.pmem.kvdk.NativeBytesHandle;
import io.pmem.kvdk.WriteBatch;

public class KVDKExamples {
    protected Configs engineConfigs;
    protected Engine kvdkEngine;

    public void prepare() throws KVDKException {
        engineConfigs = new Configs();
        engineConfigs.setHashBucketNum(1L << 10);
        engineConfigs.setMaxAccessThreads(4);

        kvdkEngine = Engine.open("/tmp/kvdk-test-dir", engineConfigs);
        engineConfigs.close();
    }

    public void runAnonymousCollection() throws KVDKException {
        System.out.println();
        System.out.println("Output of runAnonymousCollection: ");

        // put
        String key = "sssss";
        String value = "22222";
        kvdkEngine.put(key.getBytes(), value.getBytes());

        // expire
        kvdkEngine.expire(key.getBytes(), 1000);

        // get
        System.out.println("value: " + new String(kvdkEngine.get(key.getBytes())));

        // delete
        kvdkEngine.delete(key.getBytes());
    }

    public void runSortedCollection() throws KVDKException {
        System.out.println();
        System.out.println("Output of runSortedCollection: ");

        String name = "collection\u0000\nname";
        NativeBytesHandle nameHandle = new NativeBytesHandle(name.getBytes());

        // create
        kvdkEngine.sortedCreate(nameHandle);

        String key1 = "key\u00001";
        String key2 = "key\u00002";
        String key3 = "key\u00003";

        String value1 = "value\u00003";
        String value2 = "value\u00002";
        String value3 = "value\u00001";

        // disordered put
        kvdkEngine.sortedPut(nameHandle, key3.getBytes(), value3.getBytes());
        kvdkEngine.sortedPut(nameHandle, key1.getBytes(), value1.getBytes());
        kvdkEngine.sortedPut(nameHandle, key2.getBytes(), value2.getBytes());

        // print sorted result
        System.out.println("Sorted by key:");
        Iterator iter = kvdkEngine.sortedIteratorCreate(nameHandle);
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println(
                    new String(iter.key()).replace("\0", "[\\0]")
                            + ": "
                            + new String(iter.value()).replace("\0", "[\\0]"));
        }

        // close iterator
        iter.close();

        // destroy sorted collection
        kvdkEngine.sortedDestroy(nameHandle);

        // close name handle
        nameHandle.close();
    }

    public void runWriteBatch() throws KVDKException {
        System.out.println();
        System.out.println("Output of runWriteBatch: ");

        String key = "key1";
        String value1 = "value1";
        String value2 = "value2";

        // batch
        WriteBatch batch = kvdkEngine.writeBatchCreate();
        batch.stringPut(key.getBytes(), value1.getBytes());
        batch.stringPut(key.getBytes(), value2.getBytes());

        // write
        kvdkEngine.batchWrite(batch);

        // get
        kvdkEngine.get(key.getBytes());

        // print
        System.out.println("value: " + new String(kvdkEngine.get(key.getBytes())));

        // delete
        kvdkEngine.delete(key.getBytes());
    }

    public void close() {
        kvdkEngine.close();
    }

    public static void main(String[] args) {
        try {
            KVDKExamples examples = new KVDKExamples();
            examples.prepare();
            examples.runAnonymousCollection();
            examples.runSortedCollection();
            examples.runWriteBatch();
            examples.close();
        } catch (KVDKException ex) {
            ex.printStackTrace();
        }
    }
}
