package Hash;

import java.math.BigInteger;

/**
 * Provides a generic interface for representing a hash space.  Here we use the
 * "ring" metaphor that is commonly employed in DHT topologies.
 *
 * @author malensek
 */
public interface HashRing<T> {

    /**
     * Add a node to the overlay network topology.
     *
     * @param data key for the node being added
     *
     * @return the location of the new node in the hash space.
     */
    public BigInteger addNode(int nodeId, String ip, int port)
    throws HashException, HashTopologyException;

    /**
     * Determine the node that is responsible for the given data.
     *
     * @param data the data to hash against
     *
     * @return identifier of the node responsible for the provided data.
     */
    public BigInteger locate(T data) throws HashException;

}
