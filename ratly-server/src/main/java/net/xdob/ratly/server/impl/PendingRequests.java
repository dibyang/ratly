package net.xdob.ratly.server.impl;

import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.proto.raft.CommitInfoProto;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.exceptions.NotLeaderException;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.RaftClientRequest;
import net.xdob.ratly.protocol.exceptions.RaftException;
import net.xdob.ratly.protocol.RaftGroupMemberId;
import net.xdob.ratly.protocol.SetConfigurationRequest;
import net.xdob.ratly.server.config.RaftServerConfigKeys;
import net.xdob.ratly.server.metrics.RaftServerMetricsImpl;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.statemachine.TransactionContext;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.ResourceSemaphore;
import net.xdob.ratly.util.SizeInBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

class PendingRequests {
  public static final Logger LOG = LoggerFactory.getLogger(PendingRequests.class);

  private static final int ONE_MB = SizeInBytes.ONE_MB.getSizeInt();

  /**
   * Round up to the nearest MB.
   */
  static int roundUpMb(long bytes) {
    return Math.toIntExact((bytes - 1) / ONE_MB + 1);
  }

  static class Permit {}

  /**
   * The return type of {@link RequestLimits#tryAcquire(int)}.
   * The order of the enum value must match the order in {@link RequestLimits}.
   */
  enum Acquired { FAILED_IN_ELEMENT_LIMIT, FAILED_IN_BYTE_SIZE_LIMIT, SUCCESS }

  static class RequestLimits extends ResourceSemaphore.Group {
    RequestLimits(int elementLimit, int megabyteLimit) {
      super(elementLimit, megabyteLimit);
    }

    int getElementCount() {
      return get(0).used();
    }

    int getMegaByteSize() {
      return get(1).used();
    }

    Acquired tryAcquire(int messageSizeMb) {
      final int acquired = tryAcquire(1, messageSizeMb);
      return acquired == SUCCESS? Acquired.SUCCESS: Acquired.values()[acquired];
    }

    void releaseExtraMb(int extraMb) {
      release(0, extraMb);
    }

    void release(int diffMb) {
      release(1, diffMb);
    }
  }

  private static class RequestMap {
    private final Object name;
    private final ConcurrentMap<TermIndex, PendingRequest> map = new ConcurrentHashMap<>();
    private final RaftServerMetricsImpl raftServerMetrics;

    /** Permits to put new requests, always synchronized. */
    private final Map<Permit, Permit> permits = new HashMap<>();
    /** Track and limit the number of requests and the total message size. */
    private final RequestLimits resource;
    /** The size (in byte) of all the requests in this map. */
    private final AtomicLong requestSize = new AtomicLong();


    RequestMap(Object name, int elementLimit, int megabyteLimit, RaftServerMetricsImpl raftServerMetrics) {
      this.name = name;
      this.resource = new RequestLimits(elementLimit, megabyteLimit);
      this.raftServerMetrics = raftServerMetrics;

      raftServerMetrics.addNumPendingRequestsGauge(resource::getElementCount);
      raftServerMetrics.addNumPendingRequestsMegaByteSize(resource::getMegaByteSize);
    }

    /**
     * 该函数尝试为一个消息获取资源许可，用于控制请求的并发数量和内存大小限制：
     *   1. 获取消息大小并转换为MB单位；
     *   2. 调用 `resource.tryAcquire` 尝试申请资源；
     *   3. 如果因元素数或字节大小限制失败，记录指标并返回 null；
     *   4. 成功获取后更新请求总大小，并释放多余的MB资源；
     *   5. 最后创建并返回一个 Permit 对象表示成功获取资源。
     *
     */
    Permit tryAcquire(Message message) {
      final int messageSize = Message.getSize(message);
      final int messageSizeMb = roundUpMb(messageSize );
      final Acquired acquired = resource.tryAcquire(messageSizeMb);
      LOG.trace("tryAcquire {} MB? {}", messageSizeMb, acquired);
      if (acquired == Acquired.FAILED_IN_ELEMENT_LIMIT) {
        raftServerMetrics.onRequestQueueLimitHit();
        raftServerMetrics.onResourceLimitHit();
        return null;
      } else if (acquired == Acquired.FAILED_IN_BYTE_SIZE_LIMIT) {
        raftServerMetrics.onRequestByteSizeLimitHit();
        raftServerMetrics.onResourceLimitHit();
        return null;
      }

      // release extra MB
      final long oldSize = requestSize.getAndAdd(messageSize);
      final long newSize = oldSize + messageSize;
      final int diffMb = roundUpMb(newSize) - roundUpMb(oldSize);
      if (messageSizeMb > diffMb) {
        resource.releaseExtraMb(messageSizeMb - diffMb);
      }
      return putPermit();
    }

    private synchronized Permit putPermit() {
      if (resource.isClosed()) {
        return null;
      }
      final Permit permit = new Permit();
      permits.put(permit, permit);
      return permit;
    }

    synchronized PendingRequest put(Permit permit, PendingRequest p) {
      LOG.debug("{}: PendingRequests.put {}", name, p);
      final Permit removed = permits.remove(permit);
      if (removed == null) {
        return null;
      }
      Preconditions.assertTrue(removed == permit);
      final PendingRequest previous = map.put(p.getTermIndex(), p);
      Preconditions.assertTrue(previous == null);
      return p;
    }

    PendingRequest get(TermIndex termIndex) {
      final PendingRequest r = map.get(termIndex);
      LOG.debug("{}: PendingRequests.get {} returns {}", name, termIndex, r);
      return r;
    }

    PendingRequest remove(TermIndex termIndex) {
      final PendingRequest r = map.remove(termIndex);
      LOG.debug("{}: PendingRequests.remove {} returns {}", name, termIndex, r);
      if (r == null) {
        return null;
      }
      final int messageSize = Message.getSize(r.getRequest().getMessage());
      final long oldSize = requestSize.getAndAdd(-messageSize);
      final long newSize = oldSize - messageSize;
      final int diffMb = roundUpMb(oldSize) - roundUpMb(newSize);
      resource.release(diffMb);
      LOG.trace("release {} MB", diffMb);
      return r;
    }

    Collection<TransactionContext> setNotLeaderException(NotLeaderException nle,
                                                         Collection<CommitInfoProto> commitInfos) {
      synchronized (this) {
        resource.close();
        permits.clear();
      }

      LOG.debug("{}: PendingRequests.setNotLeaderException", name);
      final List<TransactionContext> transactions = new ArrayList<>(map.size());
      for(;;) {
        final Iterator<TermIndex> i = map.keySet().iterator();
        if (!i.hasNext()) { // the map is empty
          return transactions;
        }

        final PendingRequest pending = map.remove(i.next());
        if (pending != null) {
          transactions.add(pending.setNotLeaderException(nle, commitInfos));
        }
      }
    }

    void close() {
      if (raftServerMetrics != null) {
        raftServerMetrics.removeNumPendingRequestsGauge();
        raftServerMetrics.removeNumPendingRequestsByteSize();
      }
    }
  }

  private PendingRequest pendingSetConf;
  private final String name;
  private final RequestMap pendingRequests;

  PendingRequests(RaftGroupMemberId id, RaftProperties properties, RaftServerMetricsImpl raftServerMetrics) {
    this.name = id + "-" + JavaUtils.getClassSimpleName(getClass());
    this.pendingRequests = new RequestMap(id,
        RaftServerConfigKeys.Write.elementLimit(properties),
        Math.toIntExact(
            RaftServerConfigKeys.Write.byteLimit(properties).getSize()
                / SizeInBytes.ONE_MB.getSize()), //round down
        raftServerMetrics);
  }

  Permit tryAcquire(Message message) {
    return pendingRequests.tryAcquire(message);
  }

  PendingRequest add(Permit permit, RaftClientRequest request, TransactionContext entry) {
    final PendingRequest pending = new PendingRequest(request, entry);
    return pendingRequests.put(permit, pending);
  }

  PendingRequest addConfRequest(SetConfigurationRequest request) {
    Preconditions.assertTrue(pendingSetConf == null);
    pendingSetConf = new PendingRequest(request);
    return pendingSetConf;
  }

  void replySetConfiguration(Function<RaftClientRequest, RaftClientReply> newSuccessReply) {
    // we allow the pendingRequest to be null in case that the new leader
    // commits the new configuration while it has not received the retry
    // request from the client
    if (pendingSetConf != null) {
      final RaftClientRequest request = pendingSetConf.getRequest();
      LOG.debug("{}: sends success for {}", name, request);
      // for setConfiguration we do not need to wait for statemachine. send back
      // reply after it's committed.
      pendingSetConf.setReply(newSuccessReply.apply(request));
      pendingSetConf = null;
    }
  }

  void failSetConfiguration(RaftException e) {
    Preconditions.assertTrue(pendingSetConf != null);
    pendingSetConf.setException(e);
    pendingSetConf = null;
  }

  TransactionContext getTransactionContext(TermIndex termIndex) {
    final PendingRequest pendingRequest = pendingRequests.get(termIndex);
    // it is possible that the pendingRequest is null if this peer just becomes
    // the new leader and commits transactions received by the previous leader
    return pendingRequest != null ? pendingRequest.getEntry() : null;
  }

  void replyPendingRequest(TermIndex termIndex, RaftClientReply reply) {
    final PendingRequest pending = pendingRequests.remove(termIndex);
    if (pending != null) {
      Preconditions.assertEquals(termIndex, pending.getTermIndex(), "termIndex");
      pending.setReply(reply);
    }
  }

  /**
   * The leader state is stopped. Send NotLeaderException to all the pending
   * requests since they have not got applied to the state machine yet.
   */
  Collection<TransactionContext> sendNotLeaderResponses(NotLeaderException nle,
                                                        Collection<CommitInfoProto> commitInfos) {
    LOG.info("{}: sendNotLeaderResponses", name);

    final Collection<TransactionContext> transactions = pendingRequests.setNotLeaderException(nle, commitInfos);
    if (pendingSetConf != null) {
      pendingSetConf.setNotLeaderException(nle, commitInfos);
    }
    return transactions;
  }

  void close() {
    if (pendingRequests != null) {
      pendingRequests.close();
    }
  }
}
