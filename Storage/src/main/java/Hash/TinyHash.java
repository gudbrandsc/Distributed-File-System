package Hash;

import java.math.BigInteger;
import java.util.Random;

/**
 * Provides a very small (configurable) hash space for testing purposes.
 *
 * @author malensek
 */
public class TinyHash implements HashFunction<byte[]> {

    private int size = 0;
    private Checksum checksum = new Checksum();
    private Random random = new Random();

    public TinyHash(int size) {
        this.size = size;
    }

    @Override
    public BigInteger hash(byte[] data) {
        BigInteger result = new BigInteger(1, checksum.hash(data));
        result = result.mod(BigInteger.valueOf(size));
        return result;
    }

    @Override
    public BigInteger maxValue() {
        return BigInteger.valueOf(size);
    }

    @Override
    public BigInteger randomHash() {
        int randInt = random.nextInt(size);
        return BigInteger.valueOf(randInt);
    }
}
