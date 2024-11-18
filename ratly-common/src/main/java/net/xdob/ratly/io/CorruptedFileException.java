package net.xdob.ratly.io;

import java.io.File;
import java.io.IOException;

public class CorruptedFileException extends IOException {
  public CorruptedFileException(File file, String message) {
    super("File " + file + " (exist? " + file.exists() + ", length=" + file.length() + ") is corrupted: " + message);
  }
}
