package com.qlm.similitude.lsh;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.*;

/**
 * Takes an array of Strings and produces and LSH keys
 * based on the number of hash functions and the number of bands.
 * <p/>
 * NOTE: Must be Serializable for Spark.
 */
public class LshBlocking64Bit implements Serializable, LshBlockAsString {

  private HashFunction hf;
//  private static final HashFunction murmur3 = Hashing.murmur3_128();
  private static final HashFunction md5 = Hashing.md5();
  private static final BaseEncoding encoder = BaseEncoding.base64().omitPadding();
  private static final Charset UTF8 = Charset.defaultCharset();

  private final int numHashFunctions;
  private final int numRowsPerBand;
  private final int[] hashFunctions;
  private final boolean shiftKey;
  private final boolean compressKey;

  public LshBlocking64Bit(int numHashFunctions, int numRowsPerBand, boolean shiftKey, boolean compressKey, String hashAlgorithm) {
    this.compressKey = compressKey;
    this.shiftKey = shiftKey;
    this.numHashFunctions = numHashFunctions;
    this.numRowsPerBand = numRowsPerBand;
    hashFunctions = new int[numHashFunctions - 1];
    final Random random = new Random(63689);
    for (int i = 0; i < numHashFunctions - 1; i++) {
      hashFunctions[i] = random.nextInt() + 1;
    }
    if (hashAlgorithm.equalsIgnoreCase("SHA256")) {
      hf = Hashing.sha256();
    } else if (hashAlgorithm.equalsIgnoreCase("murmur3")) {
      hf = Hashing.murmur3_128();
    }
  }

  @Override
  public Set<String> lsh(String...values) {
    return new HashSet<>(bandsToStrings(lsh(hashValues(values))));
  }

  /**
   * Generates a two dimensional array where the first dimension is the band and the second dimension is the minhash rows for that band
   *
   * @param values the values to LSH
   * @return a two dimensional array where the first dimension is the band and the second dimension is the minhash rows for that band
   */
  public long[][] lsh(long[] values) {

    long[] minHash = minHash(values);

    int numBands;
    if (shiftKey) {
      numBands = numHashFunctions - numRowsPerBand + 1;
    } else {
      numBands = numHashFunctions/numRowsPerBand;
    }
    long[][] lsh = new long[numBands][numRowsPerBand];

    if (numBands == 1) {
      lsh[0] = minHash;
    } else if (shiftKey) {
      for (int i = 0; i <= numHashFunctions-numRowsPerBand; i++) {
        lsh[i] = Arrays.copyOfRange(minHash, i, numRowsPerBand+i);
      }
    } else {
        long[] tmpHash = new long[numRowsPerBand];
        int row = 0;
        for (int i = 0; i < numHashFunctions; i++) {
          tmpHash[i % numRowsPerBand] = minHash[i];
          if (i % numRowsPerBand == numRowsPerBand - 1) {
            lsh[row++] = tmpHash;
            tmpHash = new long[numRowsPerBand];
          }
        }
    }
    return lsh;
  }

  /**
   * Generates a minHash for a given set of values
   *
   * @param values the values to minHash on
   * @return An array of ints where the first int is the minimum of the actual values and the last value is the minimun of the last hash function
   */
  public long[] minHash(long[] values) {
    long[] minHash = new long[numHashFunctions];
    for (int i = 0; i < numHashFunctions; i++) {
      minHash[i] = minHashN(values, i);
    }
    return minHash;
  }

  /**
   * Hash the values from the OHOB object which are extracted by the fieldSpecs passed in the constructor
   *
   * @param values The String value to hash
   * @return An array of hash values
   */
  public long[] hashValues(String...values) {
    long[] valueHashes = new long[values.length];
    for (int i = 0; i<values.length; i++){
      valueHashes[i] = hashString(values[i]);
    }
    return valueHashes;
  }

  private long hashString(String str) {
    return hf.hashString(str, UTF8).asLong();
  }

  /**
   * Get the minimum hash for the values for the hash function designated by <code>hashFunction</code>
   *
   * @param values       The values to hash and find the minimum hash value for
   * @param hashFunction The hash function number to use. <code>0</code> results in finding the minimum from the values passed in
   * @return The minimum hash for the values
   */
  protected long minHashN(long[] values, int hashFunction) {
    long min = Long.MAX_VALUE;
    long minVal = 0;
    long tmpVal;
    for (long value : values) {
      tmpVal = getHash(value, hashFunction);
      if (tmpVal < min) {
        min = tmpVal;
        minVal = value;
      }
    }
    return minVal;
  }

  /**
   * Gets a hash value given a certain hash function.
   *
   * @param value        The value to hash
   * @param hashFunction the hash function to use
   * @return a hashCode of the given value
   */
  protected long getHash(long value, int hashFunction) {
    //For the first hash function, simply return the value
    if (hashFunction == 0) {
      return value;
    }
    long rst = (value >>> hashFunction) | (value << (Integer.SIZE - hashFunction));
    return rst ^ hashFunctions[hashFunction - 1];
  }

  List<String> bandsToStrings(long[][] lsh) {
    List<String> vals = new ArrayList<>();
    if (lsh != null) {
      for (long[] band : lsh) {
        vals.add(bandToString(band));
      }
    }
    return vals;
  }

  String bandToString(long[] hashCodes) {
    StringBuilder builder = new StringBuilder();
    if (hashCodes != null) {
      for (long i : hashCodes) {
        if (builder.length() > 0) {
          builder.append("-");
        }
        builder.append(Long.toHexString(i));
      }
    }
    String s = builder.toString();
    if (compressKey && s.length() > 0) {
      s = encoder.encode(md5.hashString(s, UTF8).asBytes());
    }
    return s;
  }

}
