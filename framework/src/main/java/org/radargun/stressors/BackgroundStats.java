package org.radargun.stressors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;
import org.radargun.CacheWrapper;
import org.radargun.state.SlaveState;

/**
 * 
 * Implements background statistics collectors and stressors. BackgroundStats don't stress the cache
 * to the fullest, they just apply mild continuous load (with wait between requests) to have some
 * statistics about cache throughput during resilience/elasticity tests.
 * 
 * @author Michal Linhard <mlinhard@redhat.com>
 * 
 */
public class BackgroundStats {
   /**
    * Bucket name for CacheWrapper requests, also key to SlaveState to retrieve BackgroundStats
    * instance and to MasterState to retrieve results.
    */
   public static final String NAME = "BackgroundStats";

   private static Logger log = Logger.getLogger(BackgroundStats.class);
   private static Random r = new Random();

   private int puts;
   private int gets;
   private int numEntries;
   private int entrySize;
   private int numThreads;
   private SlaveState slaveState;
   private long delayBetweenRequests;
   private StressorThread[] stressorThreads;
   private CacheWrapper cacheWrapper;
   private int numSlaves;
   private int slaveIndex;
   private List<Stats> stats;
   private BackgroundStatsThread backgroundStatsThread;
   private long statsIteration;
   private boolean loaded = false;

   public BackgroundStats(int puts, int gets, int numEntries, int entrySize, int numThreads, SlaveState slaveState,
         long delayBetweenRequests, int numSlaves, int slaveIndex, long statsIteration) {
      super();
      this.puts = puts;
      this.gets = gets;
      this.numEntries = numEntries;
      this.entrySize = entrySize;
      this.numThreads = numThreads;
      this.slaveState = slaveState;
      this.delayBetweenRequests = delayBetweenRequests;
      this.numSlaves = numSlaves;
      this.slaveIndex = slaveIndex;
      this.statsIteration = statsIteration;
   }

   /**
    * 
    * Returns pair [startKey, endKey] that specifies a subrange { startKey, ..., endKey-1 } of key
    * range { 0, 1, ..., numKeys-1 } divideRange divides the keyset evenly to numParts parts with
    * difference of part lengths being max 1.
    * 
    * @param numKeys
    *           Total number of keys
    * @param numParts
    *           Number of parts we're dividing to
    * @param partIdx
    *           Index of part we want to get
    * @return The pair [startKey, endKey]
    */
   public static int[] divideRange(int numKeys, int numParts, int partIdx) {
      int base = (numKeys / numParts) + 1;
      int mod = numKeys % numParts;
      if (partIdx < mod) {
         int startKey = partIdx * base;
         return new int[] { startKey, startKey + base };
      } else {
         int startKey = base * mod + (partIdx - mod) * (base - 1);
         return new int[] { startKey, startKey + base - 1 };
      }
   }

   /**
    * 
    * Starts numThreads stressors.
    * 
    */
   public void startStressors() {
      if (stressorThreads != null || cacheWrapper != null) {
         throw new IllegalStateException("Can't start stressors, they're already running.");
      }
      cacheWrapper = slaveState.getCacheWrapper();
      if (cacheWrapper == null) {
         throw new IllegalStateException("Can't start stressors, cache wrapper not available");
      }
      stressorThreads = new StressorThread[numThreads];
      int[] slaveKeyRange = divideRange(numEntries, numSlaves, slaveIndex);
      int slaveKeyRangeSize = slaveKeyRange[1] - slaveKeyRange[0];
      for (int i = 0; i < stressorThreads.length; i++) {
         int[] threadKeyRange = divideRange(slaveKeyRangeSize, numThreads, i);
         stressorThreads[i] = new StressorThread(threadKeyRange[0], threadKeyRange[1]);
         stressorThreads[i].start();
      }
   }

   /**
    * 
    * Stops the stressors, call this before tearing down or killing CacheWrapper.
    * 
    */
   public void stopStressors() {
      if (stressorThreads == null || cacheWrapper == null) {
         throw new IllegalStateException("Can't stop stressors, they're not running.");
      }
      for (int i = 0; i < stressorThreads.length; i++) {
         stressorThreads[i].interrupt();
         try {
            stressorThreads[i].join();
         } catch (InterruptedException e) {
            log.warn("interrupted while waiting for stressor to stop");
         }
      }
      stressorThreads = null;
      cacheWrapper = null;
   }

   public boolean areStressorsRunning() {
      return stressorThreads != null;
   }

   public void startStats() {
      if (backgroundStatsThread != null || stats != null) {
         throw new IllegalStateException("Stat thread already running");
      }
      stats = new ArrayList<Stats>();
      backgroundStatsThread = new BackgroundStatsThread();
      backgroundStatsThread.start();
   }

   public List<Stats> stopStats() {
      if (backgroundStatsThread == null || stats == null) {
         throw new IllegalStateException("Stat thread not running");
      }
      backgroundStatsThread.interrupt();
      try {
         backgroundStatsThread.join();
      } catch (InterruptedException e) {
         log.warn("Interrupted while waiting for stat thread to end.");
      }
      List<Stats> statsToReturn = stats;
      stats = null;
      return statsToReturn;
   }

   private class BackgroundStatsThread extends Thread {

      public void run() {
         try {
            gatherStats(); // throw away first stats
            while (true) {
               sleep(statsIteration);
               stats.add(gatherStats());
            }
         } catch (InterruptedException e) {
            log.trace("Stressor interrupted.");
         }
      }

      private Stats gatherStats() {
         if (stressorThreads == null) {
            return Stats.NODE_DOWN;
         } else {
            Stats r = null;
            for (int i = 0; i < stressorThreads.length; i++) {
               Stats threadStats = stressorThreads[i].threadStats.snapshot(true, System.currentTimeMillis());
               if (r == null) {
                  r = threadStats;
               } else {
                  r.merge(threadStats);
               }
            }
            if (r != null) {
               r.cacheSize = cacheWrapper.size();
            }
            return r;
         }
      }
   }

   private class StressorThread extends Thread {

      private int remainingGets = gets;
      private int remainingPuts = puts;
      private long lastOpStartTime;
      private Stats threadStats = new Stats();
      private int keyRangeStart;
      private int keyRangeEnd;
      private int currentKey;

      public StressorThread(int keyRangeStart, int keyRangeEnd) {
         super();
         this.keyRangeStart = keyRangeStart;
         this.keyRangeEnd = keyRangeEnd;
         this.currentKey = keyRangeStart;
      }

      @Override
      public void run() {
         try {
            if (!loaded) {
               for (currentKey = keyRangeStart; currentKey < keyRangeEnd; currentKey++) {
                  try {
                     cacheWrapper.put(NAME, key(currentKey), generateRandomString(entrySize));
                  } catch (Exception e) {
                     log.error("Error while loading data", e);
                  }
               }
               currentKey = keyRangeStart;
            }
            while (true) {
               makeRequest();
               sleep(delayBetweenRequests);
            }
         } catch (InterruptedException e) {
            log.trace("Stressor interrupted.");
         }
      }

      private String key(int key) {
         return "key" + key;
      }

      private void resetLastOpTime() {
         lastOpStartTime = System.currentTimeMillis();
      }

      private long lastOpTime() {
         return System.currentTimeMillis() - lastOpStartTime;
      }

      private void makeRequest() throws InterruptedException {
         String key = null;
         String reqDescription = null;
         boolean isPut = false;
         try {
            key = key(currentKey++);
            if (currentKey == keyRangeEnd) {
               currentKey = keyRangeStart;
            }
            if (remainingGets > 0) {
               reqDescription = "GET(" + key + ")";
               resetLastOpTime();
               Object result = cacheWrapper.get(NAME, key);
               if (result == null) {
                  throw new Exception("null result for request " + reqDescription);
               }
               threadStats.registerRequest(lastOpTime(), isPut);
               log.trace(reqDescription + " sucessfull");
               remainingGets--;
            } else if (remainingPuts > 0) {
               isPut = true;
               reqDescription = "PUT(" + key + ")";
               resetLastOpTime();
               cacheWrapper.put(NAME, key, generateRandomString(entrySize));
               threadStats.registerRequest(lastOpTime(), isPut);
               remainingPuts--;
            } else {
               throw new Exception("Both puts and gets can't be zero!");
            }
            if (remainingGets == 0 && remainingPuts == 0) {
               remainingGets = gets;
               remainingPuts = puts;
            }
         } catch (InterruptedException e) {
            throw e;
         } catch (Throwable e) {
            threadStats.registerError(lastOpTime(), isPut);
         }
      }

      private String generateRandomString(int size) {
         // each char is 2 bytes
         StringBuilder sb = new StringBuilder();
         for (int i = 0; i < size / 2; i++)
            sb.append((char) (64 + r.nextInt(26)));
         return sb.toString();
      }
   }

   public static class Stats implements Serializable {
      static final Stats NODE_DOWN = new Stats(false);

      protected boolean nodeUp = true;
      protected boolean snapshot = false;

      protected long requestsPut;
      protected long maxResponseTimePut = Long.MAX_VALUE;
      protected long responseTimeSumPut = 0;

      protected long requestsGet;
      protected long maxResponseTimeGet = Long.MAX_VALUE;
      protected long responseTimeSumGet = 0;

      protected long intervalBeginTime;
      protected long intervalEndTime;
      protected long errorsPut = 0;
      protected long errorsGet = 0;

      protected int cacheSize;

      public Stats(boolean nodeUp) {
         this.nodeUp = nodeUp;
      }

      public Stats() {
         super();
         intervalBeginTime = System.currentTimeMillis();
         intervalEndTime = intervalBeginTime;
      }

      public boolean isNodeUp() {
         return nodeUp;
      }

      public synchronized void registerRequest(long responseTime, boolean isPut) {
         ensureNotSnapshot();
         if (isPut) {
            requestsPut++;
            responseTimeSumPut += responseTime;
            if (maxResponseTimePut < responseTime) {
               maxResponseTimePut = responseTime;
            }
         } else {
            requestsGet++;
            responseTimeSumGet += responseTime;
            if (maxResponseTimeGet < responseTime) {
               maxResponseTimeGet = responseTime;
            }
         }

      }

      public synchronized void registerError(long responseTime, boolean isPut) {
         if (isPut) {
            requestsPut++;
            errorsPut++;
            responseTimeSumPut += responseTime;
            if (maxResponseTimePut < responseTime) {
               maxResponseTimePut = responseTime;
            }
         } else {
            requestsGet++;
            errorsGet++;
            responseTimeSumGet += responseTime;
            if (maxResponseTimeGet < responseTime) {
               maxResponseTimeGet = responseTime;
            }
         }
      }

      public synchronized void reset(long time) {
         ensureNotSnapshot();
         intervalBeginTime = time;
         intervalEndTime = intervalBeginTime;
         requestsPut = 0;
         requestsGet = 0;
         responseTimeSumPut = 0;
         responseTimeSumGet = 0;
         maxResponseTimePut = Long.MIN_VALUE;
         maxResponseTimeGet = Long.MIN_VALUE;
         errorsPut = 0;
         errorsGet = 0;
      }

      public synchronized Stats snapshot(boolean reset, long time) {
         ensureNotSnapshot();
         Stats result = new Stats();
         result.responseTimeSumPut = responseTimeSumPut;
         result.responseTimeSumGet = responseTimeSumGet;
         result.requestsPut = requestsPut;
         result.requestsGet = requestsGet;
         result.intervalBeginTime = intervalBeginTime;
         result.intervalEndTime = time;
         result.maxResponseTimePut = maxResponseTimePut;
         result.maxResponseTimeGet = maxResponseTimeGet;
         result.snapshot = true;
         result.errorsPut = errorsPut;
         result.errorsGet = errorsGet;
         if (reset) {
            reset(time);
         }
         return result;
      }

      public synchronized Stats copy() {
         Stats result = new Stats();
         fillCopy(result);
         return result;
      }

      protected void fillCopy(Stats result) {
         result.snapshot = snapshot;

         result.intervalBeginTime = intervalBeginTime;
         result.intervalEndTime = intervalEndTime;

         result.requestsPut = requestsPut;
         result.requestsGet = requestsGet;
         result.maxResponseTimePut = maxResponseTimePut;
         result.maxResponseTimeGet = maxResponseTimeGet;

         result.responseTimeSumPut = responseTimeSumPut;
         result.responseTimeSumGet = responseTimeSumGet;
         result.errorsPut = errorsPut;
         result.errorsGet = errorsGet;
      }

      /**
       * 
       * Merge otherStats to this. leaves otherStats unchanged.
       * 
       * @param otherStats
       */
      public synchronized void merge(Stats otherStats) {
         ensureSnapshot();
         otherStats.ensureSnapshot();
         intervalBeginTime = Math.min(otherStats.intervalBeginTime, intervalBeginTime);
         intervalEndTime = Math.max(otherStats.intervalEndTime, intervalEndTime);
         requestsPut += otherStats.requestsPut;
         requestsGet += otherStats.requestsGet;
         maxResponseTimePut = Math.max(otherStats.maxResponseTimePut, maxResponseTimePut);
         maxResponseTimeGet = Math.max(otherStats.maxResponseTimeGet, maxResponseTimeGet);
         errorsPut += otherStats.errorsPut;
         errorsGet += otherStats.errorsGet;
         responseTimeSumGet += otherStats.responseTimeSumGet;
         responseTimeSumPut += otherStats.responseTimeSumPut;
      }

      public synchronized static Stats merge(Collection<Stats> set) {
         if (set.size() == 0) {
            return null;
         }
         Iterator<Stats> elems = set.iterator();
         Stats res = elems.next().copy();
         while (elems.hasNext()) {
            res.merge(elems.next());
         }
         return res;
      }

      public boolean isSnapshot() {
         return snapshot;
      }

      protected void ensureSnapshot() {
         if (!snapshot) {
            throw new RuntimeException("this operation can be performed only on snapshot");
         }
      }

      protected void ensureNotSnapshot() {
         if (snapshot) {
            throw new RuntimeException("this operation cannot be performed on snapshot");
         }
      }

      public long getClientErrors() {
         return errorsPut + errorsGet;
      }

      public long getClientErrorsPut() {
         return errorsPut;
      }

      public long getClientErrorsGet() {
         return errorsGet;
      }

      public long getSnapshotTime() {
         return intervalEndTime;
      }

      public long getMaxResponseTimePut() {
         return maxResponseTimePut;
      }

      public long getMaxResponseTimeGet() {
         return maxResponseTimeGet;
      }

      public synchronized long getNumberOfRequests() {
         return requestsPut + requestsGet;
      }

      public synchronized long getMaxResponseTime() {
         return Math.max(maxResponseTimeGet, maxResponseTimePut);
      }

      public synchronized double getAvgResponseTimePut() {
         if (requestsPut == 0) {
            return Double.NaN;
         } else {
            return ((double) responseTimeSumPut) / ((double) requestsPut);
         }
      }

      public synchronized double getAvgResponseTimeGet() {
         if (requestsGet == 0) {
            return Double.NaN;
         } else {
            return ((double) responseTimeSumGet) / ((double) requestsGet);
         }
      }

      public synchronized double getAvgResponseTime() {
         if (getNumberOfRequests() == 0) {
            return Double.NaN;
         } else {
            return ((double) responseTimeSumPut + responseTimeSumGet) / ((double) getNumberOfRequests());
         }
      }

      public synchronized long getDuration() {
         return intervalEndTime - intervalBeginTime;
      }

      public synchronized double getThroughputPut() {
         if (getDuration() == 0) {
            return Double.NaN;
         } else {
            return ((double) requestsPut) * ((double) 1000) / ((double) getDuration());
         }
      }

      public synchronized double getThroughputGet() {
         if (getDuration() == 0) {
            return Double.NaN;
         } else {
            return ((double) requestsGet) * ((double) 1000) / ((double) getDuration());
         }
      }

      public synchronized double getThroughput() {
         if (getDuration() == 0) {
            return Double.NaN;
         } else {
            return ((double) getNumberOfRequests()) * ((double) 1000) / ((double) getDuration());
         }
      }

      @Override
      public String toString() {
         return "Stats(reqs=" + getNumberOfRequests() + ")";
      }

      public int getCacheSize() {
         return cacheSize;
      }

   }
}
