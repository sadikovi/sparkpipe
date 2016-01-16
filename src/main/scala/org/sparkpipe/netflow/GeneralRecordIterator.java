package org.sparkpipe.netflow;

import java.util.Iterator;

abstract class GeneralRecordIterator implements Iterator<Object[]> {
    public abstract boolean hasNext();

    public abstract Object[] next();

    public void remove() {
        throw new UnsupportedOperationException("Remove operation is not supported");
    }
}
