package org.sparkpipe.netflow;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.zip.InflaterInputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class CompressedRecordIterator extends GeneralRecordIterator {
    public CompressedRecordIterator(
        InflaterInputStream stream,
        NetflowRecord holder,
        ByteOrder order
    ) {
        this.RECORD_SIZE = holder.size();
        this.stream = stream;
        this.recordHolder = holder;
        this.primary = new byte[RECORD_SIZE];
        this.secondary = new byte[RECORD_SIZE];
        this.buffer = Unpooled.wrappedBuffer(this.primary).order(order);
        this.numBytes = 0;
    }

    ////////////////////////////////////////////////////////////
    // Iterator API
    ////////////////////////////////////////////////////////////
    public boolean hasNext() {
        try {
            // "read()" returns "-1", when there is EOF
            numBytes = stream.read(primary, 0, RECORD_SIZE);
            if (numBytes <= 0) {
                return false;
            }

            if (numBytes < RECORD_SIZE) {
                // we should read into secondary array
                int remaining = RECORD_SIZE - numBytes;
                int addBytes = stream.read(secondary, 0, remaining);
                if (addBytes != remaining) {
                    throw new IllegalArgumentException("Failed to read record: " +
                        addBytes + " != " + remaining);
                }
                // copy the remaning bytes into primary array
                System.arraycopy(secondary, 0, primary, numBytes, remaining);
            }
            return stream.available() > 0 && numBytes > 0;
        } catch (IOException io) {
            return false;
        }
    }

    public Object[] next() {
        return recordHolder.processRecord(buffer);
    }

    public void remove() {
        throw new UnsupportedOperationException("Remove operation is not supported");
    }

    private InflaterInputStream stream;
    private NetflowRecord recordHolder;
    private byte[] primary;
    private byte[] secondary;
    private ByteBuf buffer;
    private int numBytes;
    private int RECORD_SIZE;
}
