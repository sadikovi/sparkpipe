package org.sparkpipe.netflow;

import java.io.FilterInputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Iterator;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.hadoop.fs.FSDataInputStream;

public class RecordIterator implements Iterator<Object[]> {
    // length of the buffer, usually 32768
    public static final int BUFFER_LENGTH = 64;

    /**
     * Create RecordIterator using input stream of data.
     * @param in input stream of raw data
     * @param hl record holder, currently only V5 is supported
     * @param ord byte order to use when creating buffer to read record
     * @param isCmp boolean flag, showing that raw data is compressed
     * @return iterator of records as Object[]
     */
    public RecordIterator(FSDataInputStream in, NetflowRecord hl, ByteOrder ord, boolean isCmp) {
        RECORD_SIZE = hl.size();
        // instantiate appropriate stream for the compression
        if (isCmp) {
            stream = new InflaterInputStream(in, new Inflater(), BUFFER_LENGTH);
            compression = true;
        } else {
            stream = in;
            compression = false;
        }
        // record holder provides general methods such as "size()", and processing a record
        recordHolder = hl;
        // primary array to read bytes from
        primary = new byte[RECORD_SIZE];
        // secondary array to fill up primary in case of compression
        secondary = new byte[RECORD_SIZE];
        // record buffer, wrapped so allocated once only
        buffer = Unpooled.wrappedBuffer(primary).order(ord);
        // number of bytes read from the stream
        numBytes = 0;
    }

    ////////////////////////////////////////////////////////////
    // Iterator API
    ////////////////////////////////////////////////////////////
    public boolean hasNext() {
        boolean hasNext = true;
        try {
            numBytes = stream.read(primary, 0, RECORD_SIZE);
            // Compressed stream returns 0 to indicate EOF, and normal stream returns -1.
            if (numBytes < 0) {
                hasNext = false;
            } else if (numBytes == 0) {
                // We should not return EOF, when numBytes == 0 for uncompressed stream
                hasNext = !compression;
            } else if (numBytes < RECORD_SIZE) {
                // We have to read entire record when there is no compression, anything else is
                // considered failure. When stream is compressed we can read less, but then we need
                // buffer up remaning data.
                if (!compression) {
                    throw new IllegalArgumentException("Failed to read record: " +
                        numBytes + " < " + RECORD_SIZE);
                } else {
                    int remaining = RECORD_SIZE - numBytes;
                    int addBytes = stream.read(secondary, 0, remaining);
                    if (addBytes != remaining) {
                        throw new IllegalArgumentException("Failed to read record: " +
                            addBytes + " != " + remaining);
                    }
                    // Copy the remaning bytes into primary array
                    System.arraycopy(secondary, 0, primary, numBytes, remaining);
                }
            }

            if (!hasNext && stream != null) {
                stream.close();
            }
        } catch (IOException io) {
            System.out.println("[ERROR] " + io.getMessage());
            // TODO: log failure
            hasNext = false;
        } finally {
            return hasNext;
        }
    }

    public Object[] next() {
        return recordHolder.processRecord(buffer);
    }

    public void remove() {
        throw new UnsupportedOperationException("Remove operation is not supported");
    }

    private boolean compression;
    private FilterInputStream stream;
    private NetflowRecord recordHolder;
    private byte[] primary;
    private byte[] secondary;
    private ByteBuf buffer;
    private int numBytes;
    private int RECORD_SIZE;
}
