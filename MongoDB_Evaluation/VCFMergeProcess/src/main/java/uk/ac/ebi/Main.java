import com.datastax.driver.core.*;
//import com.datastax.driver.core.querybuilder.Batch;
//import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Strings;

import java.security.MessageDigest;
//import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
//import java.util.ArrayList;
//import java.util.List;
import java.util.Scanner;
import java.util.Vector;

import static java.lang.Math.toIntExact;


public class Main {
    private static StringBuilder headerLines = new StringBuilder();
    private static String sampleName = "";

    private static Vector<String> linesToWrite = new Vector<String>();
    private static int lineBatchIndex = 0;
    private static Session session;
    private static PreparedStatement stmt = null;

    public static void main(String[] args) {

                String[] sampleNameComps = null;
                int lineBatchSize = 50;
                String line = "";

                System.out.println(LocalDateTime.now());
                Scanner in = new Scanner(System.in);
                Cluster cluster = Cluster.builder().addContactPoint("172.22.70.150").addContactPoint("172.22.70.139").addContactPoint("172.22.70.148").addContactPoint("172.22.68.19").build();
                session = cluster.connect("variant_ksp");
                stmt = session.prepare("INSERT INTO variants (chrom,chunk,start_pos,ref,alt,qual,filter,info,sampleinfoformat,sampleinfo,var_id,var_uniq_id,sampleName) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)");
                while (in.hasNextLine()) {
                    line = in.nextLine().trim();
                    if (line.startsWith("#")) {
                        headerLines.append(line);
                        headerLines.append(System.getProperty("line.separator"));
                        if (line.startsWith("#CHROM")) {
                            sampleNameComps = line.split("\t");
                            sampleName = sampleNameComps[sampleNameComps.length - 1];
                            writeHeaderToCassandra(headerLines.toString().trim(), sampleName);
                            break;
                        }
                    }
                }
                while (in.hasNextLine()) {
                    line = in.nextLine().trim();
                    linesToWrite.add(line);
                    lineBatchIndex += 1;
                    if (lineBatchIndex == lineBatchSize) {
                        writeVariantToCassandra(linesToWrite, sampleName);
                        lineBatchIndex = 0;
                        linesToWrite.removeAllElements();
                    }
                }

                if (!linesToWrite.isEmpty()) {
                    writeVariantToCassandra(linesToWrite, sampleName);
                }

                session.close();
                cluster.close();
                System.out.println(LocalDateTime.now());

    }

    private static void writeVariantToCassandra(Vector<String> linesToWrite, String sampleName) {
        int chunkSize = 1000000;
        //Vector<String> lineDocsToWrite = new Vector<String>();
        BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
        //List<ResultSetFuture> futures = new ArrayList<ResultSetFuture>();
        for (String line :
                linesToWrite) {
            String[] lineComps = line.split("\t");
            String chromosome = lineComps[0];
            long position = Long.parseLong(lineComps[1]);
            String rsID = lineComps[2];
            String ref = lineComps[3];
            String alt = lineComps[4];
            String qual = lineComps[5];
            String qualFilter = lineComps[6];
            String info = lineComps[7];
            String sampleInfoFormat = lineComps[8];
            String sampleInfo = lineComps[9];
            String variantID = "";
            int chunk = toIntExact(position/chunkSize);
            try {
                MessageDigest md = MessageDigest.getInstance("MD5");
                variantID = chromosome + "_" + Strings.padStart(Long.toString(position), 12, '0') + "_" + new String(md.digest((ref + "_" + alt).getBytes())) + Strings.padStart(sampleName, 12, '0');
            } catch (Exception ex) {
                System.out.println(ex.getMessage());
            }
            BoundStatement boundStmt = stmt.bind(chromosome, chunk, position, ref, alt, qual, qualFilter, info, sampleInfoFormat, sampleInfo, rsID, variantID, sampleName);
            //ResultSetFuture future = session.executeAsync(boundStmt);
            //futures.add(future);
            batch.add(boundStmt);
        }
        //futures.forEach(ResultSetFuture::getUninterruptibly);
        //batch.setReadTimeoutMillis(0);
        session.execute(batch);
    }

    private static void writeHeaderToCassandra(String headerLines, String sampleName) {
        PreparedStatement headerPrepStatement = session.prepare("INSERT INTO headers (samplename, header) VALUES (?,?)");
        session.execute(headerPrepStatement.bind(sampleName, headerLines));
    }
}
