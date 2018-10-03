package Hash;

import java.math.BigInteger;
import java.util.TreeMap;

/**
 * Creates an evenly-spaced hash ring network topology.  Note that nodes in the
 * topology are spaced evenly apart, but of course this does not guarantee the
 * data will hash uniformly.
 *
 * @author malensek
 */
public class BalancedHashRing<T> implements HashRing<T> {

    /** If set to True, the first position in the hash ring is randomized. */
    protected boolean randomize = false;

    protected HashFunction<T> function;
    protected BigInteger maxHash;

    /** Maps hash ring positions to ring entries */
    protected TreeMap<BigInteger, HashRingEntry> entryMap = new TreeMap<>();

    public TreeMap<BigInteger, HashRingEntry> getEntryMap() {
        return entryMap;
    }

    public HashRingEntry getEntryAtPos(BigInteger position){
        return entryMap.get(position);
    }

    private HashRingEntry getPredecessor(HashRingEntry entry){
        for(HashRingEntry entry1 : entryMap.values()){
            if (entry1.neighbor == entry){
                return entry1;
            }
        }
        return null;
    }

    /**
     * Creates a BalancedHashRing using the provided hash function.  The
     * function will determine the size of the hash space and where nodes will
     * be placed in the topology.
     *
     * @param function HashFunction that defines the hash space being used.
     */
    public BalancedHashRing(HashFunction<T> function) {
        this(function, false);
    }

    /**
     * Creates a BalancedHashRing using the provided hash function.  The
     * function will determine the size of the hash space and where nodes will
     * be placed in the topology.
     *
     * @param function HashFunction that defines the hash space being used.
     */
    public BalancedHashRing(HashFunction<T> function, boolean randomize) {
        this.function = function;
        this.randomize = randomize;
        maxHash = function.maxValue();
    }

    /**
     * Insert a node into the hash ring internal data structures.  Nodes are
     * relinked with the proper neighbors.
     *
     * @param position place in the hash space
     * @param predecessor the predecessor node in the hash space.
     */
    private void addRingEntry(BigInteger position, HashRingEntry predecessor, int nodeId, String ip, int port)
    throws HashTopologyException {
        if (entryMap.get(position) != null) {
            /* Something is already here! */
            System.out.println(position);
            throw new HashTopologyException("Hash space exhausted!");
        }

        HashRingEntry newEntry
            = new HashRingEntry(position, predecessor.neighbor, nodeId, ip, port);
        predecessor.neighbor = newEntry;
        entryMap.put(position, newEntry);
    }

    /**
     * Insert a node into the hash ring internal data structures.  Nodes are
     * relinked with the proper neighbors.
     *
     * @param position place in the hash space
     */
    public boolean removeRingEntry(BigInteger position){
        HashRingEntry entry = entryMap.get(position);
        HashRingEntry predecessor = getPredecessor(entry);

        if (entry == null || predecessor == null) {
            System.out.println("Unable to remove node");
            return false;
        }

        predecessor.neighbor = entry.neighbor;
        entryMap.remove(position);
        System.out.println("Removed node with id: " + entry.getNodeId());
        return true;
    }

    /**
     * Add a node to the overlay network topology.
     *
     * unused for this hash ring; nodes are placed evenly based on
     * current topology characteristics.  You may safely pass 'null' to this
     * method.
     *
     * @return the location of the new node in the hash space.
     */
    @Override
    public BigInteger addNode(int nodeId, String ip, int port)
    throws HashTopologyException, HashException {
        /* Edge case: when there are no entries in the hash ring yet. */
        if (entryMap.values().size() == 0) {
            BigInteger pos;

            if (randomize) {
                /* Find a random location to start with */
                pos = function.randomHash();
            } else {
                pos = BigInteger.ZERO;
            }
            HashRingEntry firstEntry = new HashRingEntry(pos, nodeId, ip, port);
            entryMap.put(pos, firstEntry);

            return pos;
        }

        /* Edge case: only one entry in the hash ring */
        if (entryMap.values().size() == 1) {
            HashRingEntry firstEntry = entryMap.values().iterator().next();
            BigInteger halfSize = maxHash.divide(BigInteger.valueOf(2));
            BigInteger secondPos = firstEntry.position.add(halfSize);

            if (secondPos.compareTo(maxHash) > 0) {
                secondPos = secondPos.subtract(maxHash);
            }

            HashRingEntry secondEntry
                = new HashRingEntry(secondPos, firstEntry, nodeId, ip, port);
            firstEntry.neighbor = secondEntry;
            entryMap.put(secondPos, secondEntry);

            return secondPos;
        }

        /* Find the largest empty span of hash space */
        BigInteger largestSpan = BigInteger.ZERO;
        HashRingEntry largestEntry = null;
        for (HashRingEntry entry : entryMap.values()) {
            BigInteger len = lengthBetween(entry, entry.neighbor);
            if (len.compareTo(largestSpan) > 0) {
                largestSpan = len;
                largestEntry = entry;
            }
        }

        if (largestEntry == null) {
            return BigInteger.ONE.negate();
        }

        /* Put the new node in the middle of the largest span */
        BigInteger half = half(largestEntry, largestEntry.neighbor);
        addRingEntry(half, largestEntry, nodeId, ip, port);
        return half;
    }

    /**
     * Find the hash location in the middle of a hash span.  When the hash space
     * is represented as a continuous circle, a span begins at an arbitrary
     * starting point and then proceeds clockwise until it reaches its endpoint.
     *
     * @param start beginning of the hash span
     * @param end end of the hash span
     *
     * @return hash position in the middle of the span.
     */
    private BigInteger half(HashRingEntry start, HashRingEntry end) {
        BigInteger length = lengthBetween(start, end);
        /* half = start + length / 2 */
        BigInteger half
            = start.position.add(length.divide(BigInteger.valueOf(2)));

        if (maxHash.compareTo(half) >= 0) {
            return half;
        } else {
            return half.subtract(maxHash);
        }
    }

    /**
     * Determines the number of hash values between two positions on a hash
     * ring.  The hash space is viewed as a ring, starting with 0 and proceeding
     * clockwise until it reaches its maximum value.  A hash span starts at an
     * arbitrary position, and then proceeds clockwise until it reaches its
     * ending position.  The number of values between the two points is the hash
     * span length.
     *
     * @param start the start of the hash span being measured
     * @param end the end of the hash span being measured
     *
     * @return the length of the hash span
     */
    private BigInteger lengthBetween(HashRingEntry start, HashRingEntry end) {
        BigInteger difference = end.position.subtract(start.position);

        if (difference.compareTo(BigInteger.ZERO) >= 0) {
            return difference;
        } else {
            /* Wraparound */
            /* wrapped = (MAX_HASH - start) + end */
            BigInteger wrapped
                = maxHash.subtract(start.position).add(end.position);
            return wrapped;
        }
    }

    @Override
    public BigInteger locate(T data) throws HashException {
        BigInteger hashLocation = function.hash(data);
        BigInteger node = entryMap.ceilingKey(hashLocation);

        /* Wraparound edge case */
        if (node == null) {
            node = entryMap.ceilingKey(BigInteger.ZERO);
        }

        return node;
    }

    /**
     * Formats the hash ring as node-to-predecessor pair Strings.
     *
     * @return network links and nodes in String format.
     */
    @Override
    public String toString() {
        String str = "";
        HashRingEntry firstEntry = entryMap.values().iterator().next();
        HashRingEntry currentEntry = firstEntry;
        HashRingEntry nextEntry;

        do {
            nextEntry = currentEntry.neighbor;
            str += currentEntry.position + " -> " + nextEntry.position;
            str += System.lineSeparator();
            currentEntry = nextEntry;
        } while (currentEntry != firstEntry);

        return str;
    }
}
