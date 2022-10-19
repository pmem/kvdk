/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

package io.pmem.kvdk;

import java.util.Objects;

/** Java class to represent the Status enum in C++ KVDK. */
public class Status {
    private final Code code;
    private final String message;

    public Status(final Code code, final String message) {
        this.code = code;
        this.message = message;
    }

    protected Status(final byte code) {
        this(code, null);
    }

    protected Status(final byte code, final String message) {
        this.code = Code.getCode(code);
        this.message = message;
    }

    public Code getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Status status = (Status) o;
        return code == status.code && Objects.equals(message, status.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(code, message);
    }

    // should stay in sync with include/kvdk/types.h:KVDKStatus and
    // java/kvdkjni/kvdkjni.h:toJavaStatusCode
    public enum Code {
        Ok((byte) 0x0),
        NotFound((byte) 0x1),
        Outdated((byte) 0x2),
        WrongType((byte) 0x3),
        Existed((byte) 0x4),
        OperationFail((byte) 0x5),
        OutOfRange((byte) 0x6),
        MemoryOverflow((byte) 0x7),
        NotSupported((byte) 0x8),
        InvalidBatchSize((byte) 0x9),
        InvalidDataSize((byte) 0xA),
        InvalidArgument((byte) 0xB),
        IOError((byte) 0xC),
        InvalidConfiguration((byte) 0xD),
        Fail((byte) 0xE),
        Abort((byte) 0xF),
        Undefined((byte) 0x7F);

        private final byte value;

        Code(final byte value) {
            this.value = value;
        }

        public static Code getCode(final byte value) {
            for (final Code code : Code.values()) {
                if (code.value == value) {
                    return code;
                }
            }
            throw new IllegalArgumentException("Illegal value provided for Code (" + value + ").");
        }

        /**
         * Returns the byte value of the enumerations value.
         *
         * @return byte representation
         */
        public byte getValue() {
            return value;
        }
    }
}
