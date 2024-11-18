
package net.xdob.ratly.io;

import java.util.Arrays;

public interface WriteOption {
  WriteOption[] EMPTY_ARRAY = {};

  static boolean containsOption(Iterable<WriteOption> options,
                                WriteOption target) {
    for (WriteOption option : options) {
      if (option == target) {
        return true;
      }
    }

    return false;
  }

  static boolean containsOption(WriteOption[] options,
                                WriteOption target) {
    return containsOption(Arrays.asList(options), target);
  }

  default boolean isOneOf(WriteOption... options) {
    return containsOption(options, this);
  }
}
