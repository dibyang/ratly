
package net.xdob.ratly.examples.jdbc;

import net.xdob.ratly.examples.common.SubCommandBase;

import java.util.ArrayList;
import java.util.List;

public final class Db {
  private Db() {
  }

  public static List<SubCommandBase> getSubCommands() {
    List<SubCommandBase> commands = new ArrayList<>();
    commands.add(new Server());
    commands.add(new Query());
    commands.add(new Update());
    commands.add(new Check());
    commands.add(new Meta());
    commands.add(new Execute());
    return commands;
  }
}