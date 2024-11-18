
package net.xdob.ratly.shell.cli.sh.command;

import net.xdob.ratly.shell.cli.Command;
import net.xdob.ratly.shell.cli.sh.group.GroupInfoCommand;
import net.xdob.ratly.shell.cli.sh.group.GroupListCommand;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Command for the ratly group
 */
public class GroupCommand extends AbstractParentCommand {

  private static final List<Function<Context, Command>> SUB_COMMAND_CONSTRUCTORS
          = Collections.unmodifiableList(Arrays.asList(
          GroupInfoCommand::new, GroupListCommand::new));
  /**
   * @param context command context
   */
  public GroupCommand(Context context) {
    super(context, SUB_COMMAND_CONSTRUCTORS);
  }

  @Override
  public String getCommandName() {
    return "group";
  }

  @Override
  public String getDescription() {
    return description();
  }

  /**
   * @return command's description
   */
  public static String description() {
    return "Manage ratly groups; see the sub-commands for the details.";
  }
}
