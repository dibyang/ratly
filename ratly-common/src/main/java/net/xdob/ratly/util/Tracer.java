package net.xdob.ratly.util;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;

public enum Tracer {
  failed;

  final Logger logger = LoggerFactory.getLogger(this.getClass());
  private final File traceFile = Paths.get("/etc/ratly/tracer", this.name()).toFile();
  private Boolean trace;

  private Tracer() {
  }

  public boolean isTrace() {
    if (this.trace == null || !this.trace.equals(this.traceFile.exists())) {
      this.trace = this.traceFile.exists();
      this.logger.info("ratly trace {}={}", this.name(), this.trace);
    }
    return this.trace;
  }
}
