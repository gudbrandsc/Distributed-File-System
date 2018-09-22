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

    /**
     * Creates the first entry in a hash ring (neighbor is self).
     *
     * @param position the position of the entry in the hash space
     */
    public HashRingEntry(BigInteger position) {
        this.position = position;
        this.neighbor = this;
    }

    /**
     * Creates a hash ring entry with a provided position and neighbor.
     *
     * @param position the position of the entry in the hash space
     * @param neighbor neighboring entry (based on position) in the hash
     * space.
     */
    public HashRingEntry(BigInteger position, HashRingEntry neighbor) {
        this.position = position;
        this.neighbor = neighbor;
    }
}
