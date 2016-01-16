package org.sparkpipe.netflow;

import java.util.HashMap;
import io.netty.buffer.ByteBuf;

/** Generic Netflow record */
abstract class NetflowRecord {

    /**
     * Process buffer of the record and return sequence of fields in order requested. Do not handle
     * byte buffer within the method, it will be taken care of in outer caller.
     */
    public abstract byte[] processRecord(ByteBuf buffer);

    /** Size in bytes of the Netflow record (header + payload) */
    public abstract short size();

    /** Actual size of the record with only requested fields */
    public short actualSize() {
        return this.actualSize;
    }

    /** Get array of requested fields */
    public long[] getFields() {
        return this.fields;
    }

    /** Get positions of requested fields */
    public int[] getPositions() {
        return this.positions;
    }

    // actual size of the record with requested fields only
    protected short actualSize = 0;
    // array of fields
    protected long[] fields;
    // lookup array for the position of each requested field
    protected int[] positions;
}