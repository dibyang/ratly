

package net.xdob.ratly.tools;

import java.io.File;

/**
 * Default log dump tool to dump log entries for any state machine.
 */
public final class DefaultLogDump {
  private DefaultLogDump() {

  }

  public static void main(String[] args) throws Exception {
    String filePath = args[0];
    System.out.println("file path is " + filePath);
    File logFile = new File(filePath);

    ParseRatlyLog.Builder builder =  new ParseRatlyLog.Builder();
    ParseRatlyLog prl = builder.setSegmentFile(logFile)
        .build();

    prl.dumpSegmentFile();
  }
}
