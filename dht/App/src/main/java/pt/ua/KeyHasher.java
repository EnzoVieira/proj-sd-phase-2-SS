package pt.ua;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

public class KeyHasher {
    public static BigInteger hashToId(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            return new BigInteger(1, digest);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static BigInteger xorDistance(BigInteger a, BigInteger b) {
        return a.xor(b);
    }
}