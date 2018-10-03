package Hash;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Provides convenience functions for dealing with MessageDigest algorithms.
 *
 * @author malensek
 */
public class Checksum {

    private static final Logger logger = Logger.getLogger("galileo");

    private MessageDigest md;

    /**
     * Initializes a new Checksum generator using the default SHA-1 algorithm.
     */
    public Checksum() {
        /* We assume the "SHA1" algorithm is always available */
        try {
            md = MessageDigest.getInstance("SHA1");
        } catch (NoSuchAlgorithmException e) {
            logger.log(Level.SEVERE,
                    "SHA1 message digest algorithm not found!", e);
        }
    }

    /**
     * Initializes a new Checksum generator using the specified algorithm.
     *
     * @param algorithm algorithm to use to generate checksums.
     */
    public Checksum(String algorithm)
    throws NoSuchAlgorithmException {
        md = MessageDigest.getInstance(algorithm);
    }

    /**
     * Produce a checksum/hashsum of a given block of data.
     *
     * @param bytes data bytes to checksum.
     *
     * @return checksum as a byte array.
     */
    public byte[] hash(byte[] bytes) {
        return md.digest(bytes);
    }

    /**
     * Convert a hash to a hexidecimal String.
     *
     * @param hash the hash value to convert
     *
     * @return zero-padded hex String representation of the hash.
     */
    public String hashToHexString(byte[] hash) {
        BigInteger bigInt = new BigInteger(1, hash);

        /* Determine the max number of hex characters the digest will produce */
        long targetLen = md.getDigestLength() * 2;

        /* Return a formatted zero-padded String */
        return String.format("%0" + targetLen + "x", bigInt);
    }

    /**
     * Retrieves the MessageDigest instance used by this Checksum generator.
     *
     * @return the Checksum instance MessageDigest.
     */
    public MessageDigest getMessageDigest() {
        return md;
    }
}
