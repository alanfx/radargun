package org.radargun.stages;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Sleeps specified number of milliseconds.
 * 
 * @author Michal Linhard <mlinhard@redhat.com>
 */
public class SleepStage extends AbstractMasterStage {

   private static Log log = LogFactory.getLog(CsvReportGenerationStage.class);

   private long time;

   public void setTime(long time) {
      this.time = time;
   }

   public boolean execute() {
      log.trace("Sleeping " + time + " ms");
      try {
         Thread.sleep(time);
         return true;
      } catch (InterruptedException e) {
         log.warn("Sleep interrupted", e);
         return false;
      }
   }

}
