package Hash;

import java.math.BigInteger;

/**
 * Interface for mapping arbitrary data to a specific location in a hash space.
 *
 * @author malensek
 */
public interface HashFunction<T> {

    /**
     * Maps some given data to an integer location in the implemented hash
     * space.
     *
     * @param data Data to hash against.
     *
     * @return location in the hash space for the data.
     */
    public BigInteger hash(T data) throws HashException;

    /**
     * Determines the maximum hash value that this hash function can produce.
     * For example, a 160-bit SHA1 max value would be 2^160.
     *
     * @return maximum possible value this hash function will produce.
     */
    public BigInteger maxValue();

    /**
     * Returns a random location in the hash space.  This is used for seeding
     * the first nodes in an overlay, or providing random positions to place
     * nodes.
     *
     * @return random position in the hash space.
     */
    public BigInteger randomHash() throws HashException;
}
