package net.xdob.ratly.server.impl;

import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.statemachine.TransactionContext;
import com.google.common.annotations.VisibleForTesting;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.MemoizedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Managing {@link TransactionContext}.
 */
class TransactionManager {
  static final Logger LOG = LoggerFactory.getLogger(TransactionManager.class);

  private final String name;
  private final ConcurrentMap<TermIndex, MemoizedSupplier<TransactionContext>> contexts = new ConcurrentHashMap<>();

  TransactionManager(Object name) {
    this.name = name + "-" + JavaUtils.getClassSimpleName(getClass());
  }

  @VisibleForTesting
  Map<TermIndex, MemoizedSupplier<TransactionContext>> getMap() {
    LOG.debug("{}", this);
    return Collections.unmodifiableMap(contexts);
  }

  TransactionContext get(TermIndex termIndex) {
    return Optional.ofNullable(contexts.get(termIndex)).map(MemoizedSupplier::get).orElse(null);
  }

  TransactionContext computeIfAbsent(TermIndex termIndex, MemoizedSupplier<TransactionContext> constructor) {
    final MemoizedSupplier<TransactionContext> m = contexts.computeIfAbsent(termIndex, i -> constructor);
    if (!m.isInitialized()) {
      LOG.debug("{}: {}", termIndex,  this);
    }
    return m.get();
  }

  void remove(TermIndex termIndex) {
    final MemoizedSupplier<TransactionContext> removed = contexts.remove(termIndex);
    if (removed != null) {
      LOG.debug("{}: {}", termIndex,  this);
    }
  }

  @Override
  public String toString() {
    if (contexts.isEmpty()) {
      return name + " <empty>";
    }

    final StringBuilder b = new StringBuilder(name);
    contexts.forEach((k, v) -> b.append("\n  ").append(k).append(": initialized? ").append(v.isInitialized()));
    return b.toString();
  }
}