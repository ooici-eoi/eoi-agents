package net.ooici;


import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * A utility class for producing SHA1 hashes for primitive objects.  This class may also be
 * extended and used to produce SHA1 hashes for arbitray objects by implementing the
 * abstract <code>getBytes()</code> method.
 * 
 * 
 * @author tlarocque
 * @author cmueller
 */
public abstract class SHA1 {

	static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(SHA1.class);
	private static final char hexDigit[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

	/**
	 * Main-entry point (for testing hash functionality ONLY)
	 * @param args
	 */
	public static void main(String... args) {

		// long i = Long.parseLong("0111 0011     0000 0101    0001 1111    1111 1110", 2);
		// int i = Integer.parseInt("01110011000001010001111111111110", 2);
		int i = Integer.parseInt("00000001000000100000001100000100", 2);
		System.out.println(i);
		System.out.println(Integer.toBinaryString(i));
		System.out.println();

		for (byte b : getBytes32Bit(i)) {
			System.out.println(b);
		}
	}

	/**
	 * @return A representation of this object as a byte array.  This method is be used by the hashing algorithm in
	 * <code>getSHA1Hash()</code> to produce accurate SHA1 hashes.
	 */
	protected abstract byte[] getBytes();

	/**
	 * Produces a SHA1 hash of this object by invoking <code>SHA1.getSHA1Hash(byte[])</code> on the result of <code>getBytes()</code>
	 * @return The SHA1 hash of this object
	 */
	public String getSHA1Hash() {
		return SHA1.getSHA1Hash(getBytes());
	}

	/**
	 * Produces a SHA1 hash using the bytes of the given String
	 * @param s the object to hash
	 * @return the SHA1 hash
	 */
	public static String getSHA1Hash(String s) {
		return getSHA1Hash(s.getBytes());
	}

	/**
	 * Produces a SHA1 hash using the byte array representation of the given int
	 * @param n the primitive to hash
	 * @return the SHA1 hash
	 */
	public static String getSHA1Hash(int n) {
		return getSHA1Hash(getBytes32Bit(n));
	}

	/**
	 * Produces a SHA1 hash using the byte array representation of the given long
	 * @param n the primitive to hash
	 * @return the SHA1 hash
	 */
	public static String getSHA1Hash(long n) {
		return getSHA1Hash(getBytes64Bit(n));
	}

	/**
	 * Produces a SHA1 hash using the byte array representation of the given float
	 * @param n the primitive to hash
	 * @return the SHA1 hash
	 */
	public static String getSHA1Hash(float n) {
		return getSHA1Hash(getBytes32BitFloat(n));
	}

	/**
	 * Produces a SHA1 hash using the byte array representation of the given double
	 * @param n the primitive to hash
	 * @return the SHA1 hash
	 */
	public static String getSHA1Hash(double n) {
		return getSHA1Hash(getBytes64BitFloat(n));
	}

	/**
	 * Produces a SHA1 hash using the byte array representation of the given char
	 * @param n the primitive to hash
	 * @return the SHA1 hash
	 */
	public static String getSHA1Hash(char n) {
		return getSHA1Hash(getBytes32Bit(n));
	}

	/**
	 * Produces a SHA1 hash using the byte array representation of the given short
	 * @param n the primitive to hash
	 * @return the SHA1 hash
	 */
	public static String getSHA1Hash(short n) {
		return getSHA1Hash(getBytes32Bit(n));
	}

	/**
	 * Produces a SHA1 hash using the given byte
	 * @param n the primitive to hash
	 * @return the SHA1 hash
	 */
	public static String getSHA1Hash(byte n) {
		return getSHA1Hash(new byte[] { n });
	}

	/**
	 * Utilizes the java.security.MessageDigest utility to produce a SHA1 hash of the given bytes
	 * @param bytes the bytes to produce a SHA1 hash from
	 * @return the SHA1 hash
	 */
	public static String getSHA1Hash(byte[] bytes) {
		String result = null;
		try {
			MessageDigest sha = MessageDigest.getInstance("SHA1");
			result = bytesToHex(sha.digest(bytes));
		} catch (NoSuchAlgorithmException ex) {
			LOGGER.error("SHA-1 hashing algorithm is not available); bytes cannot be hashed.", ex);
		}
		return result;
	}

	/**
	 * Produces a byte array representation of the given int
	 * @param n any 32-bit integral primitive
	 * @return the byte array
	 */
	public static byte[] getBytes32Bit(int n) {
		/* @see Integer.toUnsignedString(int, int) */
		int bytePos = 4; // 32 bits / 8 bits per byte
		byte[] bytes = new byte[bytePos];

		int shift = 8; // grab byte by byte
		int radix = 1 << shift; // Ex: 0001 0000 0000
		int mask = radix - 1; // Ex: 0000 1111 1111

		do {
			byte b = (byte) (n & mask); // Zero-out insignificant bits
			bytes[--bytePos] = b; // Build the byte array backwards (least significant bytes first)
			n >>>= shift; // Move the next byte into position
		} while (n != 0);

		return bytes;
	}

	/**
	 * Produces a byte array representation of the given float
	 * @param n any 32-bit floating-point primitive
	 * @return the byte array
	 */
	public static byte[] getBytes32BitFloat(float n) {
		return getBytes32Bit(Float.floatToIntBits(n));
	}

	/**
	 * Produces a byte array representation of the given long
	 * @param n any 64-bit integral primitive
	 * @return the byte array
	 */
	public static byte[] getBytes64Bit(long n) {
		/* @see Integer.toUnsignedString(int, int) */
		int bytePos = 8; // 64 bits / 8 bits per byte
		byte[] bytes = new byte[bytePos];

		int shift = 8; // grab byte by byte
		int radix = 1 << shift; // Ex: 0001 0000 0000
		int mask = radix - 1; // Ex: 0000 1111 1111

		do {
			byte b = (byte) (n & mask); // Zero-out insignificant bits
			bytes[--bytePos] = b; // Build the byte array backwards (least significant bytes first)
			n >>>= shift; // Move the next byte into position
		} while (n != 0);

		return bytes;
	}

	/**
	 * Produces a byte array representation of the given double
	 * @param n any 64-bit floating-point primitive
	 * @return the byte array
	 */
	public static byte[] getBytes64BitFloat(double n) {
		return getBytes64Bit(Double.doubleToLongBits(n));
	}

	/**
	 * Converts the given byte array into its hexidecimal representation
	 * @param b a byte array
	 * @return a hexidecimal representation of the given byte array
	 */
	public static String bytesToHex(byte[] b) {
		StringBuilder buf = new StringBuilder();
		for (int j = 0; j < b.length; j++) {
			buf.append(hexDigit[(b[j] >> 4) & 0x0f]);
			buf.append(hexDigit[b[j] & 0x0f]);
		}


		return buf.toString();
	}
}
