package org.radargun.stages;

import java.util.ArrayList;
import java.util.List;

import org.radargun.DistStageAck;
import org.radargun.Stage;
import org.radargun.config.AdvancedStageConfigurator;
import org.radargun.state.MasterState;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Creates a test based on a timeline of events
 * 
 * @author Michal Linhard <mlinhard@redhat.com>
 */
public class TimelineStage extends AbstractDistStage {

   /**
    * Number of stressor threads active during timeline test, when all nodes are up.
    * 
    */
   private int numOfThreads = 10;

   /**
    * Timeline length (number of iterations)
    */
   private int length = 5;

   /**
    * Timeline unit (iteration duration in milliseconds)
    */
   private int unit = 1000;

   /**
    * List of timeline commands.
    */
   private List<Command> commands = new ArrayList<Command>();

   public DistStageAck executeOnSlave() {
      log.info("Executing on slave");

      List<Command> slaveCommands = getRelevantCommands(getSlaveIndex());
      StringBuffer s = new StringBuffer("Timeline for slave");
      s.append(getSlaveIndex());
      s.append("\n");
      s.append("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
      for (Command cmd : slaveCommands) {
         s.append(cmd);
         s.append("\n");
      }
      s.append("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

      return new DefaultDistStageAck(getSlaveIndex(), slaveState.getLocalAddress());
   }

   public boolean processAckOnMaster(List<DistStageAck> acks, MasterState masterState) {
      logDurationInfo(acks);
      log.info("Processing on master");
      return super.processAckOnMaster(acks, masterState);
   }

   public List<Command> getRelevantCommands(int slaveIndex) {
      List<Command> result = new ArrayList<Command>();
      for (Command cmd : commands) {
         if (cmd instanceof SlaveListCommand) {
            SlaveListCommand slaveCmd = (SlaveListCommand) cmd;
            if (slaveCmd.getSlaves().contains(slaveIndex)) {
               result.add(slaveCmd);
            }
         } else {
            result.add(cmd);
         }
      }
      return result;
   }

   @Override
   public String toString() {
      return "TimelineStage {" + "numOfThreads=" + numOfThreads + ", length=" + length + ", unit=" + unit + ", "
            + super.toString();
   }

   public static class TimelineConfigurator implements AdvancedStageConfigurator {

      @Override
      public void configure(Element stageElement, Stage stage) throws Exception {
         TimelineStage timelineStage = (TimelineStage) stage;

         NodeList childNodes = stageElement.getChildNodes();
         for (int i = 0; i < childNodes.getLength(); i++) {
            Node child = childNodes.item(i);
            if (child instanceof Element) {
               timelineStage.commands.add(element2command((Element) child));
            }
         }
      }

      private Command element2command(Element element) throws Exception {
         Command cmd = null;
         if (element.getTagName().equals("kill")) {
            cmd = new KillCommand();
         } else if (element.getTagName().equals("start")) {
            cmd = new StartCommand();
         } else {
            throw new Exception("Unknown timeline command: " + element.getTagName());
         }
         cmd.configureFrom(element);
         return cmd;
      }

   }

   public static abstract class Command {
      protected int iteration;

      public void configureFrom(Element element) throws Exception {
         String iAttrVal = element.getAttribute("i");
         if (iAttrVal == null) {
            throw new Exception("Command is missing iteration information (attribute \"i\")");
         } else {
            iteration = Integer.valueOf(iAttrVal);
         }
      }

      public int getIteration() {
         return iteration;
      }
   }

   public static abstract class SlaveListCommand extends Command {
      protected List<Integer> slaves = new ArrayList<Integer>();

      @Override
      public void configureFrom(Element element) throws Exception {
         super.configureFrom(element);
         String slavesVal = element.getAttribute("slaves");
         if (slavesVal == null) {
            throw new Exception("SlaveListCommand is missing slave information (attribute \"slaves\")");
         } else {
            String[] slaveStrings = slavesVal.split(",");
            for (String slaveString : slaveStrings) {
               slaves.add(Integer.valueOf(slaveString));
            }
         }
      }

      public List<Integer> getSlaves() {
         return slaves;
      }
   }

   public static class KillCommand extends SlaveListCommand {
      @Override
      public void configureFrom(Element element) throws Exception {
         super.configureFrom(element);
      }

      @Override
      public String toString() {
         return "Kill(i=" + iteration + ", slaves=" + slaves + ")";
      }
   }

   public static class StartCommand extends SlaveListCommand {
      @Override
      public void configureFrom(Element element) throws Exception {
         super.configureFrom(element);
      }

      @Override
      public String toString() {
         return "Start(i=" + iteration + ", slaves=" + slaves + ")";
      }
   }

}
