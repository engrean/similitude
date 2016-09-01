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
public class LshBlocking implements Serializable {

  private static final HashFunction murmur3 = Hashing.murmur3_32();
  private static final HashFunction md5 = Hashing.md5();
  private static final BaseEncoding encoder = BaseEncoding.base64().omitPadding();
  private static final Charset UTF8 = Charset.defaultCharset();

  private final int numHashFunctions;
  private final int numBands;
  private final int[] hashFunctions;
  private final boolean shiftKey;
  private final boolean compressKey;

  public LshBlocking(int numHashFunctions, int numBands) {
    this(numHashFunctions, numBands, false, false);
  }

  public LshBlocking(int numHashFunctions, int numBands, boolean shiftKey, boolean compressKey) {
    if (numHashFunctions % numBands > 0 && !shiftKey) {
      throw new IllegalArgumentException("The the number of hash functions must be evenly divisible by the number of bands");
    }
    this.compressKey = compressKey;
    this.shiftKey = shiftKey;
    this.numHashFunctions = numHashFunctions;
    this.numBands = numBands;
    hashFunctions = new int[numHashFunctions - 1];
    final Random random = new Random(63689);
    for (int i = 0; i < numHashFunctions - 1; i++) {
      hashFunctions[i] = random.nextInt() + 1;
    }
  }

  public Set<String> lsh(String...values) {
    return new HashSet<>(bandsToStrings(lsh(hashValues(values))));
  }

  /**
   * Generates a two dimensional array where the first dimension is the band and the second dimension is the minhash rows for that band
   *
   * @param values the values to LSH
   * @return a two dimensional array where the first dimension is the band and the second dimension is the minhash rows for that band
   */
  public int[][] lsh(int[] values) {
    int[][] lsh;
    if (shiftKey) {
      int numRows = numBands;
      int numKeys = numHashFunctions - numRows + 1;
      lsh = new int[numKeys][numRows];
      int[] minHash = minHash(values);
      if (numKeys > 1) {
        int row = 0;
        for (int i = 0; i < numHashFunctions-numRows; i++) {
          lsh[row++] = Arrays.copyOfRange(minHash, i, numRows+i);
        }
      } else {
        lsh[0] = minHash;
      }
    } else {
      int numRows = numHashFunctions / numBands;
      lsh = new int[numBands][numRows];
      int[] minHash = minHash(values);
      if (numBands > 1) {
        int[] tmpHash = new int[numRows];
        int row = 0;
        for (int i = 0; i < numHashFunctions; i++) {
          tmpHash[i % numRows] = minHash[i];
          if (i % numRows == numRows - 1) {
            lsh[row++] = tmpHash;
            tmpHash = new int[numRows];
          }
        }
      }
      else {
        lsh[0] = minHash;
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
  public int[] minHash(int[] values) {
    int[] minHash = new int[numHashFunctions];
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
  public int[] hashValues(String...values) {
    int[] valueHashes = new int[values.length];
    for (int i = 0; i<values.length; i++){
      valueHashes[i] = murmur3.hashString(values[i], UTF8).asInt();
    }
    return valueHashes;
  }

  /**
   * Get the minimum hash for the values for the hash function designated by <code>hashFunction</code>
   *
   * @param values       The values to hash and find the minimum hash value for
   * @param hashFunction The hash function number to use. <code>0</code> results in finding the minimum from the values passed in
   * @return The minimum hash for the values
   */
  protected int minHashN(int[] values, int hashFunction) {
    int min = Integer.MAX_VALUE;
    int minVal = 0;
    int tmpVal;
    for (int value : values) {
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
  protected int getHash(int value, int hashFunction) {
    //For the first hash function, simply return the value
    if (hashFunction == 0) {
      return value;
    }
    int rst = (value >>> hashFunction) | (value << (Integer.SIZE - hashFunction));
    return rst ^ hashFunctions[hashFunction - 1];
  }

  List<String> bandsToStrings(int[][] lsh) {
    List<String> vals = new ArrayList<>();
    if (lsh != null) {
      for (int[] band : lsh) {
        vals.add(bandToString(band));
      }
    }
    return vals;
  }

  String bandToString(int[] hashCodes) {
    StringBuilder builder = new StringBuilder();
    if (hashCodes != null) {
      for (int i : hashCodes) {
        if (builder.length() > 0) {
          builder.append("-");
        }
        builder.append(Integer.toHexString(i));
      }
    }
    String s = builder.toString();
    if (compressKey && s.length() > 0) {
      s = encoder.encode(md5.hashString(s, UTF8).asBytes());
    }
    return s;
  }

}
