/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

package io.pmem.kvdk;

/**
 * Exception class defined for KVDK.
 */
public class KVDKException extends Exception {
  private final Status status;

  public KVDKException(final String msg) {
    this(msg, null);
  }

  public KVDKException(final String msg, final Status status) {
    super(msg);
    this.status = status;
  }

  public KVDKException(final Status status) {
    super(status.getMessage() != null ? status.getMessage()
        : status.getCode().name());
    this.status = status;
  }

  public Status getStatus() {
    return status;
  }
}
