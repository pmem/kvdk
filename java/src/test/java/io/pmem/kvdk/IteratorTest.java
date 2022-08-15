/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

package io.pmem.kvdk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class IteratorTest extends EngineTestBase {

    @Test
    public void testSortedCollectionIterator() throws KVDKException {
        String name = "collection\0\nname";
        NativeBytesHandle nameHandle = new NativeBytesHandle(name.getBytes());

        // create
        kvdkEngine.sortedCreate(nameHandle);

        String key1 = "key\01";
        String key2 = "key\02";
        String key3 = "key\03";

        String value1 = "value\03";
        String value2 = "value\02";
        String value3 = "value\01";

        // disordered put
        kvdkEngine.sortedPut(nameHandle, key3.getBytes(), value3.getBytes());
        kvdkEngine.sortedPut(nameHandle, key1.getBytes(), value1.getBytes());
        kvdkEngine.sortedPut(nameHandle, key2.getBytes(), value2.getBytes());

        // create iterator
        Iterator iter = kvdkEngine.newSortedIterator(nameHandle);
        assertFalse(iter.isValid());

        // seekToFirst
        iter.seekToFirst();
        assertTrue(iter.isValid());
        assertEquals(key1, new String(iter.key()));
        assertEquals(value1, new String(iter.value()));

        // next
        iter.next();
        assertTrue(iter.isValid());
        assertEquals(key2, new String(iter.key()));
        assertEquals(value2, new String(iter.value()));

        iter.next();
        assertTrue(iter.isValid());
        assertEquals(key3, new String(iter.key()));
        assertEquals(value3, new String(iter.value()));

        iter.next();
        assertFalse(iter.isValid());

        // seekToLast
        iter.seekToLast();
        assertTrue(iter.isValid());
        assertEquals(key3, new String(iter.key()));
        assertEquals(value3, new String(iter.value()));

        // prev
        iter.prev();
        assertTrue(iter.isValid());
        assertEquals(key2, new String(iter.key()));
        assertEquals(value2, new String(iter.value()));

        iter.prev();
        assertTrue(iter.isValid());
        assertEquals(key1, new String(iter.key()));
        assertEquals(value1, new String(iter.value()));

        iter.prev();
        assertFalse(iter.isValid());

        // close iterator
        iter.close();

        // destroy sorted collection
        kvdkEngine.sortedDestroy(nameHandle);

        // close name handle
        nameHandle.close();
    }
}
