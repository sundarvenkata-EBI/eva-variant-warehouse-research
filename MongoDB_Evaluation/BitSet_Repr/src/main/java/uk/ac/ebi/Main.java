package uk.ac.ebi;
import java.security.Timestamp;
import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.*;
import java.time.LocalDateTime;
import org.apache.commons.codec.binary.Base64;
import me.lemire.integercompression.*;
import me.lemire.integercompression.differential.*;

public class Main {

    private static long[] BinEncodeIntArray(int[] inputArr, int numSamp)
    {
        int extraAlloc = 0;
        if ((numSamp & 31) > 0) {extraAlloc = 1;}
        long[] resultArray = new long[(numSamp>>5)+extraAlloc];
        for (int sampleIndex: inputArr) {
            int offset = sampleIndex>>5;
            int lsbIndex = 32 - (sampleIndex & 31) - 1;
            resultArray[offset] = resultArray[offset] | (1L << lsbIndex);
        }
        return resultArray;
    }

    private static int getBit(long n, int k) {
        return (int) ((n >> k) & 1L);
    }

    private static List<Long> BinDecodeIntArray(long[] binArr, int numSamp)
    {
        List<Long> resultArray = new ArrayList<Long>();
        int len = binArr.length;
        for (int i = 0; i < len; i++) {
            long currElem = binArr[i];
            for (int j = 31; j >= 0; j--) {
                if (getBit(currElem, j) == 1)
                {
                    resultArray.add(Long.valueOf (31 - j + (i<<5)));
                }
            }
        }
        return resultArray;
    }

    private static int[] generateRandomArray(int n){
        int[] list = new int[n];
        Random random = new Random();

        for (int i = 0; i < n; i++)
        {
            list[i] = (random.nextInt(n));
        }
        return list;
    }

    public static void main(String[] args) {
//        BitSet b = new BitSet(1000000);
//        b.set(2);
//        b.set(3);
//        b.set(999999);
//        BitSet c =BitSet.valueOf(b.toByteArray());
//        c.set(4);
//        String s = new String(Base64.encodeBase64String(b.toByteArray()));
//        System.out.println(s);
//        System.out.println(c);
        long cumEncodeExecTime = 0;
        long cumDecodeExecTime = 0;
        for (int i = 0; i < 10000; i++) {
            Random random = new Random();
            int numElems = random.nextInt(200);
            int[] inputArr = generateRandomArray(numElems);
            LocalDateTime startTime = LocalDateTime.now();
            long[] resultArr = BinEncodeIntArray(inputArr, 200);
            LocalDateTime endTime = LocalDateTime.now();
            cumEncodeExecTime += Duration.between (startTime, endTime).getNano();

            startTime = LocalDateTime.now();
            BinDecodeIntArray(resultArr, 200);
            endTime = LocalDateTime.now();
            cumDecodeExecTime += Duration.between (startTime, endTime).getNano();
        }

        System.out.println(cumEncodeExecTime) ;
        System.out.println(cumDecodeExecTime) ;
//        int[] inputArr = new int[]{1,3,7,15,31,32,64};
//        long[] binArr = BinEncodeIntArray(inputArr, 65);
//        System.out.println(Arrays.toString(binArr));
//        System.out.println(Arrays.toString(BinDecodeIntArray(binArr, 65).toArray()));

//        IntegratedIntCompressor iic = new IntegratedIntCompressor();
//        int[] data = new int[]{4,14,16,25,30,48,58,61,64,66,76,77,84,85,90,97,112,118,120,126,128,144,160,161,165,181,183,193,200,206,210,212,216,217,221,222,231,241,245,265,267,268,271,272,274,275,278,281,284,285,288,291,292,293,296,317,333,336,342,346,351,357,358,369,373,381,399,407,412,415,417,425,437,447,482,498,518,522,527,534,544,547,553,556,560,577,580,588,592,593,594,601,618,621,622,626,629,632,633,644,649,651,652,654,655,658,660,661,662,665,670,671,681,707,715,717,744,745,746,748,754,763,765,766,773,776,781,782,784,786,800,801,802,803,808,820,822,824,825,826,827,828,829,840,847,849,853,892,901,909,911,914,917,918,919,935,938,962,969,1024,1025,1026,1040,1041,1048,1057,1059,1079,1083,1120,1146,1152,1240,1348,1353,1359,1365,1369,1373,1395,1402,1414,1430,1431,1442,1449,1450,1451,1458,1459,1462,1466,1469,1471,1477,1480,1483,1490,1492,1493,1495,1505,1512,1515,1519,1529,1531,1535,1542,1549,1550,1560,1565,1568,1578,1584,1594,1599,1605,1611,1617,1624,1626,1630,1641,1651,1652,1656,1662,1666,1667,1668,1678,1681,1685,1687,1695,1698,1699,1701,1704,1718,1723,1726,1733,1737,1744,1745,1747,1786,1790,1791,1792,1796,1798,1799,1801,1802,1806,1810,1812,1821,1826,1832,1839,1844,1857,1862,1866,1908,1911,1920,1926,1930,1933,1939,1943,1944,1946,1954,1957,1964,1972,1974,2000,2003,2020,2067,2175,2201,2202,2217,2221,2230,2235,2240,2295,2305,2311,2312,2317,2320,2326,2329,2330,2332,2335,2343,2362,2367,2373,2384,2395,2396,2406,2413,2414,2421,2422,2423,2433,2439,2440,2449,2465,2468,2475,2476,2481,2486,2487,2495,2503};
//        for(int k = 0; k < data.length; ++k)
//            data[k] = k;
//        System.out.println("Compressing "+data.length+" integers using friendly interface");
//        int[] compressed = iic.compress(data);
//        int[] recov = iic.uncompress(compressed);
//        System.out.println(recov.length);
//        System.out.println("compressed from "+data.length*4/1024+"KB to "+compressed.length*4+" bytes");
//        if(!Arrays.equals(recov,data)) throw new RuntimeException("bug");
    }
}
