/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

package io.pmem.kvdk;

import org.junit.Test;

public class IteratorTest extends EngineTestBase {

  @Test
  public void testSortedCollectionIterator() throws KVDKException {
    String name = "collection\0\nname";
    NativeBytesHandle nameHandle = new NativeBytesHandle(name.getBytes());

    // create
    kvdkEngine.sortedCreate(nameHandle);

    String key = "key\01";
    String value1 = "value\01";
    String value2 = "value\02";
    String value3 = "value\03";

    // put
    kvdkEngine.sortedPut(nameHandle, key.getBytes(), value3.getBytes());
    kvdkEngine.sortedPut(nameHandle, key.getBytes(), value1.getBytes());
    kvdkEngine.sortedPut(nameHandle, key.getBytes(), value2.getBytes());

    // create iterator
    Iterator iter = kvdkEngine.newSortedIterator(nameHandle);

    // TODO check order of values

    // close iterator
    iter.close();

    // destroy sorted collection
    kvdkEngine.sortedDestroy(nameHandle);

    // close name handle
    nameHandle.close();
  }
}
