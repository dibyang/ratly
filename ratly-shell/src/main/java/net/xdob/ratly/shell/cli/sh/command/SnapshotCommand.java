
package net.xdob.ratly.shell.cli.sh.command;

import net.xdob.ratly.shell.cli.Command;
import net.xdob.ratly.shell.cli.sh.snapshot.TakeSnapshotCommand;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Command for the ratly snapshot
 */
public class SnapshotCommand extends AbstractParentCommand {
  private static final List<Function<Context, Command>> SUB_COMMAND_CONSTRUCTORS
      = Collections.unmodifiableList(Arrays.asList(TakeSnapshotCommand::new));

  /**
   * @param context command context
   */
  public SnapshotCommand(Context context) {
    super(context, SUB_COMMAND_CONSTRUCTORS);
  }

  @Override
  public String getCommandName() {
    return "snapshot";
  }

  @Override
  public String getDescription() {
    return description();
  }

  /**
   * @return command's description
   */
  public static String description() {
    return "Manage ratly snapshot; see the sub-commands for the details.";
  }
}
