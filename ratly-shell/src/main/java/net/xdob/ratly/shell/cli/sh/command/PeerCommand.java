
package net.xdob.ratly.shell.cli.sh.command;

import net.xdob.ratly.shell.cli.Command;
import net.xdob.ratly.shell.cli.sh.peer.AddCommand;
import net.xdob.ratly.shell.cli.sh.peer.RemoveCommand;
import net.xdob.ratly.shell.cli.sh.peer.SetPriorityCommand;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Command for the ratly peer
 */
public class PeerCommand extends AbstractParentCommand{

  private static final List<Function<Context, Command>> SUB_COMMAND_CONSTRUCTORS
      = Collections.unmodifiableList(Arrays.asList(AddCommand::new, RemoveCommand::new,
      SetPriorityCommand::new));

  /**
   * @param context command context
   */
  public PeerCommand(Context context) {
    super(context, SUB_COMMAND_CONSTRUCTORS);
  }

  @Override
  public String getCommandName() {
    return "peer";
  }

  @Override
  public String getDescription() {
    return description();
  }

  /**
   * @return command's description
   */
  public static String description() {
    return "Manage ratly peers; see the sub-commands for the details.";
  }
}
