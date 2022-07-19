/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

package io.pmem.kvdk;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.pmem.kvdk.Status.Code;

public class EngineTest {
  private Engine kvdkEngine;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void init() throws KVDKException, IOException {
    File tempDir = folder.newFolder();
    String enginePath = tempDir.getAbsolutePath();

    Configs engineConfigs = new Configs();
    engineConfigs.setHashBucketNum(1L << 10);
    engineConfigs.setMaxAccessThreads(4);
    engineConfigs.setPMemSegmentBlocks(2L << 20);
    engineConfigs.setPMemFileSize(1L << 30); // 1 GB
    engineConfigs.setPopulatePMemSpace(false);

    kvdkEngine = Engine.open(enginePath, engineConfigs);
  }

  @After
  public void teardown() {
    kvdkEngine.close();
  }

  @Test
  public void testOpenAndCloseKVDKEngine() {
  }

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

    // duplicate creation: OK
    kvdkEngine.sortedCreate(nameHandle);

    // destroy
    kvdkEngine.sortedDestroy(nameHandle);

    // create again
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
}
