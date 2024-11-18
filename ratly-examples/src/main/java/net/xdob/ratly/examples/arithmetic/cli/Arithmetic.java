
package net.xdob.ratly.examples.arithmetic.cli;

import net.xdob.ratly.examples.common.SubCommandBase;

import java.util.ArrayList;
import java.util.List;

/**
 * This class enumerates all the commands enqueued by Arithmetic state machine.
 */
public final class Arithmetic {
  private Arithmetic() {
  }

  public static List<SubCommandBase> getSubCommands() {
    List<SubCommandBase> commands = new ArrayList<>();
    commands.add(new Server());
    commands.add(new Assign());
    commands.add(new Get());
    return commands;
  }
}