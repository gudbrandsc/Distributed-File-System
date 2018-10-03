package Hash;

import java.math.BigInteger;

/**
 * Represents a node entry in a hash ring.  This implementation is
 * singly-linked; nodes maintain a reference to their successor only.
 */
public class HashRingEntry {

    /** Position in the hash space */
    public BigInteger position;

    /** Next neighboring node in the hash space */
    public HashRingEntry neighbor;

    private int nodeId;

    private String ip;

    private int port;

    /**
     * Creates the first entry in a hash ring (neighbor is self).
     *
     * @param position the position of the entry in the hash space
     */
    public HashRingEntry(BigInteger position, int nodeId, String ip, int port) {
        this.position = position;
        this.neighbor = this;
        this.nodeId = nodeId;
        this.ip = ip;
        this.port = port;
    }

    /**
     * Creates a hash ring entry with a provided position and neighbor.
     *
     * @param position the position of the entry in the hash space
     * @param neighbor neighboring entry (based on position) in the hash
     * space.
     */
    public HashRingEntry(BigInteger position, HashRingEntry neighbor, int nodeId, String ip, int port) {
        this.position = position;
        this.neighbor = neighbor;
        this.nodeId = nodeId;
        this.ip = ip;
        this.port = port;
    }

    public BigInteger getPosition() {
        return position;
    }


    public int getNodeId() {
        return nodeId;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }
}
