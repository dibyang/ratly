
package net.xdob.ratly.shell.cli.sh.command;

import net.xdob.ratly.shell.cli.Command;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class AbstractParentCommand implements Command {
  private final Map<String, Command> subs;

  protected AbstractParentCommand(Context context, List<Function<Context, Command>> subCommandConstructors) {
    this.subs = Collections.unmodifiableMap(subCommandConstructors.stream()
        .map(constructor -> constructor.apply(context))
        .collect(Collectors.toMap(Command::getCommandName, Function.identity(),
        (a, b) -> {
          throw new IllegalStateException("Found duplicated commands: " + a + " and " + b);
          }, LinkedHashMap::new)));
  }

  @Override
  public final Map<String, Command> getSubCommands() {
    return subs;
  }

  @Override
  public final String getUsage() {
    final StringBuilder usage = new StringBuilder(getCommandName());
    for (String cmd : getSubCommands().keySet()) {
      usage.append(" [").append(cmd).append("]");
    }
    return usage.toString();
  }
}
