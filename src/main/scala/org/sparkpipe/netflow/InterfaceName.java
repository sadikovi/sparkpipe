package org.sparkpipe.netflow;

import java.nio.ByteOrder;

/** Interface name as one of the TLV */
public class InterfaceName {
    public InterfaceName(long ip, int ifIndex, String ifName) {
        this.ip = ip;
        this.ifIndex = ifIndex;
        this.ifName = ifName;
    }

    /** Get interface IP */
    public long getIP() {
        return this.ip;
    }

    /** Get interface IfIndex */
    public int getIfIndex() {
        return this.ifIndex;
    }

    /** Get interface name */
    public String getInterfaceName() {
        return this.ifName;
    }

    private long ip = 0;
    private int ifIndex = 0;
    private String ifName = null;
}
