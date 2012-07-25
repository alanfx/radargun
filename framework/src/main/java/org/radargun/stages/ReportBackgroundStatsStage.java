package org.radargun.stages;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.Format;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.reporting.CSVChart;
import org.radargun.stressors.BackgroundStats;
import org.radargun.stressors.BackgroundStats.Stats;

/**
 * 
 * Generates reports from BackgroundStats results.
 * 
 * @author Michal Linhard <mlinhard@redhat.com>
 * 
 */
public class ReportBackgroundStatsStage extends AbstractMasterStage {
   public static final Format NUMFORMAT = new DecimalFormat("0.000");
   public static final int CHART_W = 800;
   public static final int CHART_H = 600;

   private static Log log = LogFactory.getLog(ReportBackgroundStatsStage.class);

   private String targetDir = "reports";

   public boolean execute() {
      @SuppressWarnings("unchecked")
      List<List<Stats>> results = (List<List<Stats>>) masterState.get(BackgroundStats.NAME);
      if (results == null) {
         log.error("Could not find BackgroundStats results on the master. Master's state is  " + masterState);
         return false;
      }
      if (results.size() == 0) {
         log.warn("Nothing to report!");
         return false;
      }
      if (masterState.getSlavesCountForCurrentStage() != results.size()) {
         log.error("We're missing statistics from some slaves");
         return false;
      }
      try {
         File reportDir = new File(targetDir);
         if (!reportDir.exists() && !reportDir.mkdirs()) {
            log.error("Couldn't create directory " + targetDir);
            return false;
         }
         File subdir = new File(reportDir, masterState.nameOfTheCurrentBenchmark() + "_"
               + masterState.configNameOfTheCurrentBenchmark() + "_" + results.size());
         if (!subdir.exists() && !subdir.mkdirs()) {
            log.error("Couldn't create directory " + subdir.getAbsolutePath());
            return false;
         }
         int maxResultSize = -1;
         for (int i = 0; i < results.size(); i++) {
            if (maxResultSize < results.get(i).size()) {
               maxResultSize = results.get(i).size();
            }
         }

         File csvThroughput = new File(subdir, "throughput.csv");
         File csvAvgRespTimes = new File(subdir, "avg-response-times.csv");
         File csvEntryCounts = new File(subdir, "entry-counts.csv");

         generateMultiSlaveCsv(csvThroughput, results, maxResultSize, new StatGetter() {
            @Override
            public String getStat(Stats cell) {
               if (cell.isNodeUp()) {
                  return ffcsv(cell.getThroughput());
               } else {
                  return "0.0";
               }
            }
         });
         generateMultiSlaveCsv(csvAvgRespTimes, results, maxResultSize, new StatGetter() {
            @Override
            public String getStat(Stats cell) {
               if (cell.isNodeUp()) {
                  return ffcsv(cell.getAvgResponseTime());
               } else {
                  return "0.0";
               }
            }
         });
         generateMultiSlaveCsv(csvEntryCounts, results, maxResultSize, new StatGetter() {
            @Override
            public String getStat(Stats cell) {
               if (cell.isNodeUp()) {
                  return Integer.toString(cell.getCacheSize());
               } else {
                  return "0";
               }
            }
         });

         CSVChart.writeCSVAsChart("Throughput on slaves", "Iteration", "Throughput (ops/sec)",
               csvThroughput.getAbsolutePath(), CSVChart.SEPARATOR, "Iteration", getSlaveNames(), CHART_W, CHART_H,
               csvThroughput.getAbsolutePath() + ".png");
         CSVChart.writeCSVAsChart("Average response times", "Iteration", "Average response time (ms)",
               csvAvgRespTimes.getAbsolutePath(), CSVChart.SEPARATOR, "Iteration", getSlaveNames(), CHART_W, CHART_H,
               csvAvgRespTimes.getAbsolutePath() + ".png");
         CSVChart.writeCSVAsChart("Entry counts in slaves", "Iteration", "Number of entries",
               csvEntryCounts.getAbsolutePath(), CSVChart.SEPARATOR, "Iteration", getSlaveNames(), CHART_W, CHART_H,
               csvEntryCounts.getAbsolutePath() + ".png");

         return true;
      } catch (Exception e) {
         log.error("Error while generating CSV from BackgroundStats", e);
         return false;
      }
   }

   private static String ffcsv(double val) {
      return (Double.isNaN(val) || val == Double.MAX_VALUE || val == Double.MIN_VALUE) ? CSVChart.NULL : NUMFORMAT
            .format(val);
   }

   private List<String> getSlaveNames() {
      List<String> result = new ArrayList<String>();
      for (int i = 0; i < masterState.getSlavesCountForCurrentStage(); i++) {
         result.add("slave" + i);
      }
      return result;
   }

   private void generateMultiSlaveCsv(File file, List<List<Stats>> results, int maxResultSize, StatGetter getter)
         throws Exception {
      PrintWriter w = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF-8"));
      w.print("Iteration");
      List<String> slaveNames = getSlaveNames();
      for (int i = 0; i < slaveNames.size(); i++) {
         w.print(CSVChart.SEPARATOR);
         w.print(slaveNames.get(i));
      }
      w.println();
      for (int i = 0; i < maxResultSize; i++) {
         w.print(i);
         for (int j = 0; j < results.size(); j++) {
            w.print(CSVChart.SEPARATOR);
            List<Stats> statList = results.get(j);
            if (i < statList.size()) {
               w.print(getter.getStat(statList.get(i)));
            } else {
               w.print(CSVChart.NULL);
            }
         }
         w.println();
      }
      w.close();
   }

   private interface StatGetter {
      String getStat(Stats cell);
   }

   public void setTargetDir(String targetDir) {
      this.targetDir = targetDir;
   }

}
