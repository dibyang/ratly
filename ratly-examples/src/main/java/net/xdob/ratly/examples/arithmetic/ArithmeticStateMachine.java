
package net.xdob.ratly.examples.arithmetic;

import net.xdob.ratly.examples.arithmetic.expression.Expression;
import net.xdob.ratly.io.MD5Hash;
import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.proto.raft.RaftPeerRole;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.RaftGroupId;
import net.xdob.ratly.server.RaftServer;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.server.storage.FileInfo;
import net.xdob.ratly.server.storage.RaftStorage;
import net.xdob.ratly.statemachine.StateMachineStorage;
import net.xdob.ratly.statemachine.TransactionContext;
import net.xdob.ratly.statemachine.impl.BaseStateMachine;
import net.xdob.ratly.statemachine.impl.SimpleStateMachineStorage;
import net.xdob.ratly.statemachine.impl.SingleFileSnapshotInfo;
import net.xdob.ratly.util.AutoCloseableLock;
import net.xdob.ratly.util.FileUtils;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.MD5FileUtil;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ArithmeticStateMachine extends BaseStateMachine {
  private final Map<String, Double> variables = new ConcurrentHashMap<>();

  private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

  private AutoCloseableLock readLock() {
    return AutoCloseableLock.acquire(lock.readLock());
  }

  private AutoCloseableLock writeLock() {
    return AutoCloseableLock.acquire(lock.writeLock());
  }

  void reset() {
    variables.clear();
    setLastAppliedTermIndex(null);
  }

  @Override
  public void initialize(RaftServer server, RaftGroupId groupId,
      RaftStorage raftStorage) throws IOException {
    super.initialize(server, groupId, raftStorage);
    this.storage.init(raftStorage);
    loadSnapshot(storage.getLatestSnapshot());
  }

  @Override
  public void reinitialize() throws IOException {
    close();
    loadSnapshot(storage.loadLatestSnapshot());
  }

  @Override
  public long takeSnapshot() {
    final Map<String, Double> copy;
    final TermIndex last;
    try(AutoCloseableLock readLock = readLock()) {
      copy = new HashMap<>(variables);
      last = getLastAppliedTermIndex();
    }

    final File snapshotFile =  storage.getSnapshotFile(last.getTerm(), last.getIndex());
    LOG.info("Taking a snapshot to file {}", snapshotFile);

    try(ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(
        FileUtils.newOutputStream(snapshotFile)))) {
      out.writeObject(copy);
    } catch(IOException ioe) {
      LOG.warn("Failed to write snapshot file \"" + snapshotFile
          + "\", last applied index=" + last);
    }

    final MD5Hash md5 = MD5FileUtil.computeAndSaveMd5ForFile(snapshotFile);
    final FileInfo info = new FileInfo(snapshotFile.toPath(), md5);
    storage.updateLatestSnapshot(new SingleFileSnapshotInfo(info, last));
    return last.getIndex();
  }

  public long loadSnapshot(SingleFileSnapshotInfo snapshot) throws IOException {
    if (snapshot == null) {
      LOG.warn("The snapshot info is null.");
      return RaftLog.INVALID_LOG_INDEX;
    }
    final File snapshotFile = snapshot.getFile().getPath().toFile();
    if (!snapshotFile.exists()) {
      LOG.warn("The snapshot file {} does not exist for snapshot {}", snapshotFile, snapshot);
      return RaftLog.INVALID_LOG_INDEX;
    }

    // verify md5
    final MD5Hash md5 = snapshot.getFile().getFileDigest();
    if (md5 != null) {
      MD5FileUtil.verifySavedMD5(snapshotFile, md5);
    }

    final TermIndex last = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(snapshotFile);
    try(AutoCloseableLock writeLock = writeLock();
        ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(
            FileUtils.newInputStream(snapshotFile)))) {
      reset();
      setLastAppliedTermIndex(last);
      variables.putAll(JavaUtils.cast(in.readObject()));
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Failed to load " + snapshot, e);
    }
    return last.getIndex();
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return storage;
  }

  @Override
  public CompletableFuture<Message> query(Message request) {
    final Expression q = Expression.Utils.bytes2Expression(request.getContent().toByteArray(), 0);
    final Double result;
    try(AutoCloseableLock readLock = readLock()) {
      result = q.evaluate(variables);
    }
    final Expression r = Expression.Utils.double2Expression(result);
    LOG.debug("QUERY: {} = {}", q, r);
    return CompletableFuture.completedFuture(Expression.Utils.toMessage(r));
  }

  @Override
  public void close() {
    reset();
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    final LogEntryProto entry = trx.getLogEntryUnsafe();
    final AssignmentMessage assignment = new AssignmentMessage(entry.getStateMachineLogEntry().getLogData());

    final long index = entry.getIndex();
    final Double result;
    try(AutoCloseableLock writeLock = writeLock()) {
      result = assignment.evaluate(variables);
      updateLastAppliedTermIndex(entry.getTerm(), index);
    }
    final Expression r = Expression.Utils.double2Expression(result);
    final CompletableFuture<Message> f = CompletableFuture.completedFuture(Expression.Utils.toMessage(r));

    final RaftPeerRole role = trx.getServerRole();
    if (role == RaftPeerRole.LEADER) {
      LOG.info("{}:{}-{}: {} = {}", role, getId(), index, assignment, r);
    } else {
      LOG.debug("{}:{}-{}: {} = {}", role, getId(), index, assignment, r);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("{}-{}: variables={}", getId(), index, variables);
    }
    return f;
  }
}
