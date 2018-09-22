package Hash;

import java.math.BigInteger;
import java.util.Random;

/**
 * Provides an SHA1 HashFunction.
 *
 * @author malensek
 */
public class SHA1 implements HashFunction<byte[]> {

    private Checksum checksum = new Checksum();
    private Random random = new Random();

    @Override
    public synchronized BigInteger hash(byte[] data) throws HashException {
        return new BigInteger(1, checksum.hash(data));
    }

    @Override
    public BigInteger maxValue() {
        int hashBytes = checksum.getMessageDigest().getDigestLength();
        return BigInteger.valueOf(2).pow(hashBytes * 8);
    }

    @Override
    public synchronized BigInteger randomHash() throws HashException {
        byte[] randomBytes = new byte[1024];
        random.nextBytes(randomBytes);
        return hash(randomBytes);
    }
}
