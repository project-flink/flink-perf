package com.github.projectflink.generators;

import org.apache.commons.math3.random.RandomDataGenerator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class Utils {

    private static String[] realWordsDict;
    private static String[] userAgents;

    static {
        BufferedReader br;
        // get real words
        try {
            URL rsrc = Logdata.class.getResource("/dictionary.txt");
            System.err.println("rs="+rsrc);
            br = new BufferedReader( new InputStreamReader(rsrc.openStream()));
            String line;
            List<String> el = new ArrayList<String>();
            while ((line = br.readLine()) != null) {
                el.add(line.trim());
            }
            br.close();
            realWordsDict = el.toArray(new String[el.size()]);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // get user agents
        try {
            URL rsrc = Logdata.class.getResource("/ua.txt");
            System.err.println("rs="+rsrc);
            br = new BufferedReader( new InputStreamReader(rsrc.openStream()));
            String line;
            List<String> el = new ArrayList<String>();
            while ((line = br.readLine()) != null) {
                el.add(line.trim());
            }
            br.close();
            userAgents = el.toArray(new String[el.size()]);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String[] getDictionary() {
        return realWordsDict;
    }

    public static String[] getUAs() {
        return userAgents;
    }

    public static String getRandomRealWord(Random rnd) {
        return realWordsDict[rnd.nextInt(realWordsDict.length-1)];
    }

    private static RandomDataGenerator rndZipf = new RandomDataGenerator();
    public static String getSlowZipfRandomWord() {
        int idx = rndZipf.nextZipf(realWordsDict.length, 0.77);
        return realWordsDict[idx];
    }

    private static ZipfDistribution fastZipf = new ZipfDistribution(1.5, realWordsDict.length);
    public static String getFastZipfRandomWord() {
        int idx = (int)fastZipf.next();
        return realWordsDict[idx];
    }

    public static String getRandomUA(Random rnd) {
        return userAgents[rnd.nextInt(userAgents.length-1)];
    }


    //
    // Util classes
    //
    public static class XORShiftRandom extends Random {
        private long seed = System.nanoTime();

        public XORShiftRandom() {
        }
        protected int next(int nbits) {
            // N.B. Not thread-safe!
            long x = this.seed;
            x ^= (x << 21);
            x ^= (x >>> 35);
            x ^= (x << 4);
            this.seed = x;
            x &= ((1L << nbits) -1);
            return (int) x;
        }
    }

    public static class ZipfDistribution {
        private double skew;
        private long maxVal;
        private double[] sumProbCache;
        private double normalizationConstant = 0.0;
        private Random rand = new XORShiftRandom();

        public ZipfDistribution(double skew, long maxVal) {
            this.skew = skew;
            this.maxVal = maxVal;

            // Compute normalization constant on first call only
            for (long i = 1; i <= this.maxVal; ++i) {
                normalizationConstant = normalizationConstant +
                        (1.0 / Math.pow((double) i, skew));
            }
            normalizationConstant = 1.0f / normalizationConstant;

            sumProbCache = new double[(int)this.maxVal];
            for (int i = 0; i < this.maxVal; ++i) {
                this.sumProbCache[i] = -1.0f;
            }
        }

        public long next() {
            double z; // Uniform random number (0 < z < 1)
            double sumProb; // Sum of probabilities
            int zipfValue = -1; // Computed exponential value to be returned

            // Pull a uniform random number (0 < z < 1)
            do {
                z = rand.nextDouble();
            } while (z == 0.0f);

            // Map z to the value
            sumProb = 0;
            for (int i = 1; i <= this.maxVal; i++) {
                if (sumProbCache[i - 1] < 0.0f) {
                    sumProb = sumProb + normalizationConstant / Math.pow((double) i, skew);
                    sumProbCache[i - 1] = sumProb;
                } else {
                    sumProb = sumProbCache[i - 1];
                }

                if (sumProb >= z) {
                    zipfValue = i;
                    break;
                }
            }
            return zipfValue - 1;
        }
    }

}
