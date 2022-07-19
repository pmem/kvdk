/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

package io.pmem.kvdk.examples;

import io.pmem.kvdk.Configs;
import io.pmem.kvdk.Engine;
import io.pmem.kvdk.KVDKException;

public class KVDKExamples {
  protected Configs engineConfigs;
  protected Engine kvdkEngine;

  public void prepare() throws KVDKException {
    engineConfigs = new Configs();
    engineConfigs.setHashBucketNum(1L << 10);
    engineConfigs.setMaxAccessThreads(4);
    engineConfigs.setPMemSegmentBlocks(2L << 20);
    engineConfigs.setPMemFileSize(1L << 30); // 1 GB
    engineConfigs.setPopulatePMemSpace(false);

    kvdkEngine = Engine.open("/tmp/kvdk-test-dir", engineConfigs);
    engineConfigs.close();
  }

  public void run() throws KVDKException {
    // put
    String key = "sssss";
    String value = "22222";
    kvdkEngine.put(key.getBytes(), value.getBytes());

    // get
    System.out.println("value: " + new String(kvdkEngine.get(key.getBytes())));

    // delete
    kvdkEngine.delete(key.getBytes());

    // close
    kvdkEngine.close();
  }

  public static void main(String[] args) {
    try {
      KVDKExamples examples = new KVDKExamples();
      examples.prepare();
      examples.run();
    } catch (KVDKException ex) {
      ex.printStackTrace();
    }
  }
}
