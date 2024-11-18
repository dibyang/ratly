
package net.xdob.ratly.shell.cli.sh.command;

import net.xdob.ratly.shell.cli.Command;

import java.io.PrintStream;

/**
 * The base class for all the ratly shell {@link Command} classes.
 */
public abstract class AbstractCommand implements Command {

  private final Context context;

  protected AbstractCommand(Context context) {
    this.context = context;
  }

  protected Context getContext() {
    return context;
  }

  protected PrintStream getPrintStream() {
    return getContext().getPrintStream();
  }

  protected void printf(String format, Object... args) {
    getPrintStream().printf(format, args);
  }

  protected void println(Object message) {
    getPrintStream().println(message);
  }
}
