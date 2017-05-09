package uk.ac.ebi;
import java.util.BitSet;
import org.apache.commons.codec.binary.Base64;

public class Main {

    public static void main(String[] args) {
        BitSet b = new BitSet(1000000);
        b.set(2);
        b.set(3);
        b.set(999999);
        BitSet c =BitSet.valueOf(b.toByteArray());
        c.set(4);
        String s = new String(Base64.encodeBase64String(b.toByteArray()));
        System.out.println(s);
        System.out.println(c);
    }
}
