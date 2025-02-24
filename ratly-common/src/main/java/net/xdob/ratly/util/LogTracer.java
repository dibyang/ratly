package net.xdob.ratly.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;

public class LogTracer {

  final Logger LOG = LoggerFactory.getLogger(this.getClass());
  private final String name;
  private final File traceFile;
  private Boolean trace;

  public LogTracer(String name) {
    this.name = name;
    this.traceFile = Paths.get("/etc/ratly/trace", name).toFile();
  }

  public String getName() {
    return name;
  }

  public boolean isTrace(){
    if(trace==null||!trace.equals(this.traceFile.exists())){
      trace = this.traceFile.exists();
      LOG.info("trace {}={}", this.getName(), trace);
    }
    return trace;
  }

  public static LogTracer c(String name){
    return new LogTracer(name);
  }
}
