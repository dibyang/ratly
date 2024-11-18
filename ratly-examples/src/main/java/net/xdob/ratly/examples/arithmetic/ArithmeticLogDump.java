

package net.xdob.ratly.examples.arithmetic;

import java.io.File;

import net.xdob.ratly.proto.RaftProtos.StateMachineLogEntryProto;
import net.xdob.ratly.tools.ParseRatlyLog;

/**
 * Utility to dump log segments for Arithmetic State Machine.
 */
public final class ArithmeticLogDump {
  private ArithmeticLogDump() {

  }

  public static void main(String[] args) throws Exception {
    String filePath = args[0];
    System.out.println("file path is " + filePath);
    File logFile = new File(filePath);

    ParseRatlyLog.Builder builder =  new ParseRatlyLog.Builder();
    ParseRatlyLog prl = builder.setSegmentFile(logFile)
        .setSMLogToString(ArithmeticLogDump::smToArithmeticLogString)
        .build();

    prl.dumpSegmentFile();
  }

  private static String smToArithmeticLogString(StateMachineLogEntryProto logEntryProto) {
    AssignmentMessage message = new AssignmentMessage(logEntryProto.getLogData());
    return message.toString();
  }
}
