package net.xdob.ratly.util.function;

import net.xdob.ratly.util.UncheckedAutoCloseable;

import java.util.function.Supplier;

/**
 * A {@link Supplier} which is also {@link UncheckedAutoCloseable}.
 *
 * @param <T> the type of the {@link Supplier}.
 */
public interface UncheckedAutoCloseableSupplier<T> extends UncheckedAutoCloseable, Supplier<T> {
}
