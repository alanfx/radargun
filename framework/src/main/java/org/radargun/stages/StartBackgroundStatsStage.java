package org.radargun.stages;

import org.radargun.DistStageAck;
import org.radargun.stressors.BackgroundStats;

/**
 * 
 * Create BackgroundStats and store them to SlaveState.
 * 
 * @author Michal Linhard <mlinhard@redhat.com>
 */
public class StartBackgroundStatsStage extends AbstractDistStage {

   private int puts = 1;
   private int gets = 2;
   private int numEntries = 1024;
   private int entrySize = 1024;
   private int numThreads = 10;
   private long delayBetweenRequests;
   private long statsIterationDuration = 5000;

   @Override
   public DistStageAck executeOnSlave() {
      DefaultDistStageAck ack = newDefaultStageAck();
      try {
         BackgroundStats bgStats = new BackgroundStats(puts, gets, numEntries, entrySize, numThreads, slaveState,
               delayBetweenRequests, getActiveSlaveCount(), getSlaveIndex(), statsIterationDuration);
         if (slaveState.getCacheWrapper() != null) {
            bgStats.startStressors();
         }
         bgStats.startStats();
         slaveState.put(BackgroundStats.NAME, bgStats);
         return ack;
      } catch (Exception e) {
         log.error("Error while starting background stats");
         ack.setError(true);
         ack.setRemoteException(e);
         return ack;
      }
   }

   public void setPuts(int puts) {
      this.puts = puts;
   }

   public void setGets(int gets) {
      this.gets = gets;
   }

   public void setNumEntries(int numEntries) {
      this.numEntries = numEntries;
   }

   public void setEntrySize(int entrySize) {
      this.entrySize = entrySize;
   }

   public void setNumThreads(int numThreads) {
      this.numThreads = numThreads;
   }

   public void setDelayBetweenRequests(long delayBetweenRequests) {
      this.delayBetweenRequests = delayBetweenRequests;
   }

   public void setStatsIterationDuration(long statsIterationDuration) {
      this.statsIterationDuration = statsIterationDuration;
   }

}
