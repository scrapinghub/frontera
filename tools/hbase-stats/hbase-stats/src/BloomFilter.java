import java.io.Serializable;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;


public class BloomFilter<E> implements Serializable {
    private OpenBitSet bitset;
    private long bitSetSize;
    private double bitsPerElement;
    private long expectedNumberOfFilterElements;
    private long numberOfAddedElements;
    private int k;

    static final Charset charset = Charset.forName("UTF-8");

    static final String hashName = "MD5";
    static final MessageDigest digestFunction;

    static {
        MessageDigest tmp;
        try {
            tmp = java.security.MessageDigest.getInstance(hashName);
        } catch (NoSuchAlgorithmException e) {
            tmp = null;
        }
        digestFunction = tmp;
    }

    public BloomFilter(double c, long n, int k) {
        this.expectedNumberOfFilterElements = n;
        this.k = k;
        this.bitsPerElement = c;
        this.bitSetSize = (long) Math.ceil(c * n);
        numberOfAddedElements = 0;
        this.bitset = new OpenBitSet(bitSetSize);
    }

    public BloomFilter(long bitSetSize, long expectedNumberOElements) {
        this(bitSetSize / (double) expectedNumberOElements,
                expectedNumberOElements,
                (int) Math.round((bitSetSize / (double) expectedNumberOElements) * Math.log(2.0)));
    }

    public BloomFilter(double falsePositiveProbability, long expectedNumberOfElements) {
        this(Math.ceil(-(Math.log(falsePositiveProbability) / Math.log(2))) / Math.log(2), // c = k / ln(2)
                expectedNumberOfElements,
                (int) Math.ceil(-(Math.log(falsePositiveProbability) / Math.log(2)))); // k = ceil(-log_2(false prob.))
    }

    public BloomFilter(long bitSetSize, long expectedNumberOfFilterElements, long actualNumberOfFilterElements, OpenBitSet filterData) {
        this(bitSetSize, expectedNumberOfFilterElements);
        this.bitset = filterData;
        this.numberOfAddedElements = actualNumberOfFilterElements;
    }

    public static long createHash(String val, Charset charset) {
        return createHash(val.getBytes(charset));
    }

    public static long createHash(String val) {
        return createHash(val, charset);
    }

    public static long createHash(byte[] data) {
        return createHashes(data, 1)[0];
    }

    public static long[] createHashes(byte[] data, int hashes) {
        long[] result = new long[hashes];

        int k = 0;
        byte salt = 0;
        while (k < hashes) {
            byte[] digest;
            synchronized (digestFunction) {
                digestFunction.update(salt);
                salt++;
                digest = digestFunction.digest(data);
            }

            for (int i = 0; i < digest.length / 8 && k < hashes; i++) {
                int h = 0;
                for (int j = (i * 8); j < (i * 8) + 8; j++) {
                    h <<= 8;
                    h |= ((int) digest[j]) & 0xFF;
                }
                result[k] = h;
                k++;
            }
        }
        return result;
    }

    public double expectedFalsePositiveProbability() {
        return getFalsePositiveProbability(expectedNumberOfFilterElements);
    }

    public double getFalsePositiveProbability(double numberOfElements) {
        return Math.pow((1 - Math.exp(-k * (double) numberOfElements
                / (double) bitSetSize)), k);

    }

    public double getFalsePositiveProbability() {
        return getFalsePositiveProbability(numberOfAddedElements);
    }

    public int getK() {
        return k;
    }

    public void add(E element) {
        add(element.toString().getBytes(charset));
    }

    public void add(byte[] bytes) {
        long[] hashes = createHashes(bytes, k);
        for (long hash : hashes)
            bitset.set(Math.abs(hash % bitSetSize));
        numberOfAddedElements++;
    }

    public void addAll(Collection<? extends E> c) {
        for (E element : c)
            add(element);
    }

    public boolean contains(E element) {
        return contains(element.toString().getBytes(charset));
    }

    public boolean contains(byte[] bytes) {
        long[] hashes = createHashes(bytes, k);
        for (long hash : hashes) {
            if (!bitset.get(Math.abs(hash % bitSetSize))) {
                return false;
            }
        }
        return true;
    }

    public boolean containsAll(Collection<? extends E> c) {
        for (E element : c)
            if (!contains(element))
                return false;
        return true;
    }

    public boolean getBit(int bit) {
        return bitset.get(bit);
    }

    public OpenBitSet getBitSet() {
        return bitset;
    }

    public long size() {
        return this.bitSetSize;
    }

    public long count() {
        return this.numberOfAddedElements;
    }

    public long getExpectedNumberOfElements() {
        return expectedNumberOfFilterElements;
    }

    public double getExpectedBitsPerElement() {
        return this.bitsPerElement;
    }

    public double getBitsPerElement() {
        return this.bitSetSize / (double) numberOfAddedElements;
    }
}
