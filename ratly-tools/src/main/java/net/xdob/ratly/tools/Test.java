package net.xdob.ratly.tools;

import java.io.File;
import java.io.IOException;

public class Test {
  public static void main(String[] args) throws IOException {
    String filePath = "D:/test/current/log_inprogress_11313406";//"log_5491710-5544923";
    System.out.println("file path is " + filePath);
    File logFile = new File(filePath);

    ParseRatlyLog.Builder builder =  new ParseRatlyLog.Builder();
    ParseRatlyLog prl = builder.setSegmentFile(logFile)
        .build();

    prl.dumpSegmentFile();
  }
}
