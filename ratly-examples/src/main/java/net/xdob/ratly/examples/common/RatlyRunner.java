
package net.xdob.ratly.examples.common;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import net.xdob.ratly.examples.arithmetic.cli.Arithmetic;
import net.xdob.ratly.examples.filestore.cli.FileStore;
import net.xdob.ratly.examples.jdbc.Db;
import net.xdob.ratly.util.JavaUtils;

import java.util.List;
import java.util.Optional;

/**
 * Standalone raft server.
 */
public final class RatlyRunner {

  private RatlyRunner() {

  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.err.println("No command type specified: ");
      return;
    }
    List<SubCommandBase> commands = initializeCommands(args[0]);
    RatlyRunner ratlyRunner = new RatlyRunner();

    if (commands == null) {
      System.err.println("Wrong command type: " + args[0]);
      return;
    }
    String[] newArgs = new String[args.length - 1];
    System.arraycopy(args, 1, newArgs, 0, args.length - 1);

    JCommander.Builder builder = JCommander.newBuilder().addObject(ratlyRunner);
    commands.forEach(command -> builder
        .addCommand(JavaUtils.getClassSimpleName(command.getClass()).toLowerCase(), command));
    JCommander jc = builder.build();
    try {
      jc.parse(newArgs);
      final Optional<SubCommandBase> selectedCommand = commands.stream()
          .filter(command -> JavaUtils.getClassSimpleName(command.getClass()).equalsIgnoreCase(jc.getParsedCommand()))
          .findFirst();
      if (selectedCommand.isPresent()) {
        selectedCommand.get().run();
      } else {
        jc.usage();
      }
    } catch (ParameterException exception) {
      System.err.println("Wrong parameters: " + exception.getMessage());
      jc.usage();
    }

  }

  private static List<SubCommandBase> initializeCommands(String command) {
    if (command.equalsIgnoreCase(JavaUtils.getClassSimpleName(FileStore.class))) {
      return FileStore.getSubCommands();
    } else if (command.equalsIgnoreCase(JavaUtils.getClassSimpleName(Arithmetic.class))) {
      return Arithmetic.getSubCommands();
    } else if (command.equalsIgnoreCase(JavaUtils.getClassSimpleName(Db.class))) {
      return Db.getSubCommands();
    }
    return null;
  }

}
