package org.sparkpipe.netflow;

import java.util.HashMap;
import io.netty.buffer.ByteBuf;

public class NetflowV5Record extends NetflowRecord {
    // list of supported columns and size in bytes
    // Current seconds since 0000 UTC 1970, size: 4
    public static final long V5_FIELD_UNIX_SECS = 0x00000001L;
    // Residual nanoseconds since 0000 UTC 1970, size: 4
    public static final long V5_FIELD_UNIX_NSECS = 0x00000002L;
    // Current time in millisecs since router booted, size: 4
    public static final long V5_FIELD_SYSUPTIME = 0x00000004L;
    // Exporter IP address, size: 4
    public static final long V5_FIELD_EXADDR = 0x00000008L;
    // Source IP Address, size: 4
    public static final long V5_FIELD_SRCADDR = 0x00000010L;
    // Destination IP Address, size: 4
    public static final long V5_FIELD_DSTADDR = 0x00000020L;
    // Next hop router's IP Address, size: 4
    public static final long V5_FIELD_NEXTHOP = 0x00000040L;
    // Input interface index, size: 2
    public static final long V5_FIELD_INPUT = 0x00000080L;
    // Output interface index, size: 2
    public static final long V5_FIELD_OUTPUT = 0x00000100L;
    // Packets sent in Duration, size: 4
    public static final long V5_FIELD_DPKTS = 0x00000200L;
    // Octets sent in Duration, size: 4
    public static final long V5_FIELD_DOCTETS = 0x00000400L;
    // SysUptime at start of flow, size: 4
    public static final long V5_FIELD_FIRST = 0x00000800L;
    // and of last packet of flow, size: 4
    public static final long V5_FIELD_LAST = 0x00001000L;
    // TCP/UDP source port number or equivalent, size: 2
    public static final long V5_FIELD_SRCPORT = 0x00002000L;
    // TCP/UDP destination port number or equiv, size: 2
    public static final long V5_FIELD_DSTPORT = 0x00004000L;
    // IP protocol, e.g., 6=TCP, 17=UDP, ..., size: 1
    public static final long V5_FIELD_PROT = 0x00008000L;
    // IP Type-of-Service, size: 1
    public static final long V5_FIELD_TOS = 0x00010000L;
    // OR of TCP header bits, size: 1
    public static final long V5_FIELD_TCP_FLAGS = 0x00020000L;
    // Type of flow switching engine (RP,VIP,etc.), size: 1
    public static final long V5_FIELD_ENGINE_TYPE = 0x00040000L;
    // Slot number of the flow switching engine, size: 1
    public static final long V5_FIELD_ENGINE_ID = 0x00080000L;
    // mask length of source address, size: 1
    public static final long V5_FIELD_SRC_MASK = 0x00100000L;
    // mask length of destination address, size: 1
    public static final long V5_FIELD_DST_MASK = 0x00200000L;
    // AS of source address, size: 2
    public static final long V5_FIELD_SRC_AS = 0x00400000L;
    // AS of destination address, size: 2
    public static final long V5_FIELD_DST_AS = 0x00800000L;

    public NetflowV5Record(long[] askedFields) {
        int len = askedFields.length;
        this.fields = askedFields;
        this.positions = new int[len];
        long fld;
        for (int i=0; i<len; i++) {
            fld = this.fields[i];
            // update position for the field, which is actual size before update
            this.positions[i] = this.actualSize;
            // increment size
            if (fld == V5_FIELD_UNIX_SECS || fld == V5_FIELD_UNIX_NSECS ||
                fld == V5_FIELD_SYSUPTIME || fld == V5_FIELD_EXADDR || fld == V5_FIELD_SRCADDR ||
                fld == V5_FIELD_DSTADDR || fld == V5_FIELD_NEXTHOP || fld == V5_FIELD_DPKTS ||
                fld == V5_FIELD_DOCTETS || fld == V5_FIELD_FIRST || fld == V5_FIELD_LAST) {
                this.actualSize += 4;
            } else if (fld == V5_FIELD_INPUT || fld == V5_FIELD_OUTPUT ||
                fld == V5_FIELD_SRCPORT || fld == V5_FIELD_DSTPORT || fld == V5_FIELD_SRC_AS ||
                fld == V5_FIELD_DST_AS) {
                this.actualSize += 2;
            } else if (fld == V5_FIELD_PROT || fld == V5_FIELD_TOS || fld == V5_FIELD_TCP_FLAGS ||
                fld == V5_FIELD_ENGINE_TYPE || fld == V5_FIELD_ENGINE_ID ||
                fld == V5_FIELD_SRC_MASK || fld == V5_FIELD_DST_MASK) {
                this.actualSize += 1;
            } else {
                throw new UnsupportedOperationException("Field " + fld + " does not exist");
            }
        }
    }

    public byte[] processRecord(ByteBuf buffer) {
        // initialize a new record
        byte[] record = new byte[this.actualSize];
        // go through each field and fill up buffer
        int len = this.fields.length;
        for (int i=0; i<len; i++) {
            writeField(this.fields[i], buffer, this.positions[i], record);
        }
        return record;
    }

    // copy bytes from buffer to the record
    private void writeField(long fld, ByteBuf buffer, int pos, byte[] record) {
        if (fld == V5_FIELD_UNIX_SECS) {
            buffer.getBytes(0, record, pos, 4);
        } else if (fld == V5_FIELD_UNIX_NSECS) {
            buffer.getBytes(4, record, pos, 4);
        } else if (fld == V5_FIELD_SYSUPTIME) {
            buffer.getBytes(8, record, pos, 4);
        } else if (fld == V5_FIELD_EXADDR) {
            buffer.getBytes(12, record, pos, 4);
        } else if (fld == V5_FIELD_SRCADDR) {
            buffer.getBytes(16, record, pos, 4);
        } else if (fld == V5_FIELD_DSTADDR) {
            buffer.getBytes(20, record, pos, 4);
        } else if (fld == V5_FIELD_NEXTHOP) {
            buffer.getBytes(24, record, pos, 4);
        } else if (fld == V5_FIELD_INPUT) {
            buffer.getBytes(28, record, pos, 2);
        } else if (fld == V5_FIELD_OUTPUT) {
            buffer.getBytes(30, record, pos, 2);
        } else if (fld == V5_FIELD_DPKTS) {
            buffer.getBytes(32, record, pos, 4);
        } else if (fld == V5_FIELD_DOCTETS) {
            buffer.getBytes(36, record, pos, 4);
        } else if (fld == V5_FIELD_FIRST) {
            buffer.getBytes(40, record, pos, 4);
        } else if (fld == V5_FIELD_LAST) {
            buffer.getBytes(44, record, pos, 4);
        } else if (fld == V5_FIELD_SRCPORT) {
            buffer.getBytes(48, record, pos, 2);
        } else if (fld == V5_FIELD_DSTPORT) {
            buffer.getBytes(50, record, pos, 2);
        } else if (fld == V5_FIELD_PROT) {
            buffer.getBytes(52, record, pos, 1);
        } else if (fld == V5_FIELD_TOS) {
            buffer.getBytes(53, record, pos, 1);
        } else if (fld == V5_FIELD_TCP_FLAGS) {
            buffer.getBytes(54, record, pos, 1);
        } else if (fld == V5_FIELD_ENGINE_TYPE) {
            // there is field "pad" which is unused byte in record, we skip it
            buffer.getBytes(56, record, pos, 1);
        } else if (fld == V5_FIELD_ENGINE_ID) {
            buffer.getBytes(57, record, pos, 1);
        } else if (fld == V5_FIELD_SRC_MASK) {
            buffer.getBytes(58, record, pos, 1);
        } else if (fld == V5_FIELD_DST_MASK) {
            buffer.getBytes(59, record, pos, 1);
        } else if (fld == V5_FIELD_SRC_AS) {
            buffer.getBytes(60, record, pos, 2);
        } else if (fld == V5_FIELD_DST_AS) {
            buffer.getBytes(62, record, pos, 2);
        }
    }

    /** Size in bytes of the Netflow V5 record */
    public short size() {
        return 64;
    }
}
