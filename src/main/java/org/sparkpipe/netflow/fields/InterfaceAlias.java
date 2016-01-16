package org.sparkpipe.netflow.fields;

import java.nio.ByteOrder;

/** Interface alias as one of the TLV */
public class InterfaceAlias {
    public InterfaceAlias(long ip, int ifIndexCount, int ifIndex, String aliasName) {
        this.ip = ip;
        this.ifIndexCount = ifIndexCount;
        this.ifIndex = ifIndex;
        this.aliasName = aliasName;
    }

    /** Get IP of device */
    public long getIP() {
        return this.ip;
    }

    /** Get ifIndex count */
    public int getIfIndexCount() {
        return this.ifIndexCount;
    }

    /** Get ifIndex of interface */
    public int getIfIndex() {
        return this.ifIndex;
    }

    /** Get alias name */
    public String getAliasName() {
        return this.aliasName;
    }

    private long ip = 0;
    private int ifIndexCount = 0;
    private int ifIndex = 0;
    private String aliasName = null;
}
