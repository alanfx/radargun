package org.radargun.stages;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Validates cluster via JMX
 * 
 * @author Michal Linhard <mlinhard@redhat.com>
 */
public class JMXClusterValidationStage extends AbstractMasterStage {

   private static Log log = LogFactory.getLog(JMXClusterValidationStage.class);

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
