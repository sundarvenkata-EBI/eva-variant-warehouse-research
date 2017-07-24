import com.google.common.base.Strings;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.time.LocalDateTime;
import java.util.Scanner;
import java.util.Vector;


public class WriteRecs {
    private static String headerLines = "";
    private static String sampleName = "";

    private static Vector<String> linesToWrite = new Vector<String>();
    private static int lineBatchIndex = 0;

    public static void main(String[] args) {
        String[] sampleNameComps = null;
        int lineBatchSize = 1000;
        String line = "";

        //System.out.println(LocalDateTime.now());
        Scanner in = new Scanner(System.in);
//        Cluster cluster = Cluster.builder().addContactPoint("172.22.70.150").addContactPoint("172.22.70.139").addContactPoint("172.22.70.148").addContactPoint("172.22.68.19").build();
//        session = cluster.connect("variant_ksp");
//        stmt = session.prepare("INSERT INTO variants (chrom,start_pos,ref,alt,qual,filter,info,sampleinfoformat,sampleinfo,var_id,var_uniq_id,sampleName) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)");
        while (in.hasNextLine()) {
            line = in.nextLine().trim();
            if (line.startsWith("#")) {
                if (line.startsWith("#CHROM")) {
                    sampleNameComps = line.split("\t");
                    sampleName = sampleNameComps[sampleNameComps.length - 1];
                }
                continue;
            }
            linesToWrite.add(line);
            lineBatchIndex += 1;
            if (lineBatchIndex == lineBatchSize) {
                writeVariantToFile(linesToWrite, sampleName);
                lineBatchIndex = 0;
                linesToWrite.removeAllElements();
            }
        }

        if (!linesToWrite.isEmpty()) {
            writeVariantToFile(linesToWrite, sampleName);
        }

        //session.close();
        //cluster.close();
        //System.out.println(LocalDateTime.now());
    }

    private static void writeVariantToFile(Vector<String> linesToWrite, String sampleName) {
        for (String line :
                linesToWrite) {

            String[] lineComps = line.split("\t");
            String chromosome = lineComps[0];
            long position = Long.parseLong(lineComps[1]);

            String ref = lineComps[3];
            String alt = lineComps[4];

            String variantID = "";
            try {
                MessageDigest md = MessageDigest.getInstance("MD5");
                variantID = chromosome + "_" + Strings.padStart(Long.toString(position), 12, '0') + "_" + DatatypeConverter.printHexBinary(md.digest((ref + "_" + alt).getBytes())) + Strings.padStart(sampleName, 12, '0');
            } catch (Exception ex) {
                System.out.println(ex.getMessage());
            }
            System.out.println(line + "\t" + sampleName + "\t" + variantID);
        }
    }
}
