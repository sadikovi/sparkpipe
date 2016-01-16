package org.sparkpipe.netflow;

import java.io.IOException;
import java.nio.ByteOrder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.hadoop.fs.FSDataInputStream;

public class RecordIterator extends GeneralRecordIterator {
    public RecordIterator(FSDataInputStream stm, NetflowRecord holder, ByteOrder order) {
        this.RECORD_SIZE = holder.size();
        this.stm = stm;
        this.recordHolder = holder;
        this.chunk = new byte[RECORD_SIZE];
        this.buffer = Unpooled.wrappedBuffer(this.chunk).order(order);
        this.numBytes = 0;
    }

    ////////////////////////////////////////////////////////////
    // Iterator API
    ////////////////////////////////////////////////////////////
    public boolean hasNext() {
        try {
            numBytes = stm.read(chunk, 0, chunk.length);
            if (numBytes >= 0 && numBytes < RECORD_SIZE) {
                throw new UnsupportedOperationException("Failed to read record: " +
                    numBytes + " < " + RECORD_SIZE);
            }
            return numBytes >= 0;
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

    private int RECORD_SIZE;
    private FSDataInputStream stm;
    private NetflowRecord recordHolder;
    private byte[] chunk;
    private ByteBuf buffer;
    private int numBytes;
}
