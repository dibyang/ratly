
package net.xdob.ratly.client;

import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.SizeInBytes;
import net.xdob.ratly.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static net.xdob.ratly.conf.ConfUtils.*;

public interface RaftClientConfigKeys {
  Logger LOG = LoggerFactory.getLogger(RaftClientConfigKeys.class);

  static Consumer<String> getDefaultLog() {
    return LOG::debug;
  }

  String PREFIX = "raft.client";

  interface Rpc {
    String PREFIX = RaftClientConfigKeys.PREFIX + ".rpc";

    String REQUEST_TIMEOUT_KEY = PREFIX + ".request.timeout";
    TimeDuration REQUEST_TIMEOUT_DEFAULT = TimeDuration.valueOf(60, TimeUnit.SECONDS);
    static TimeDuration requestTimeout(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(REQUEST_TIMEOUT_DEFAULT.getUnit()),
          REQUEST_TIMEOUT_KEY, REQUEST_TIMEOUT_DEFAULT, getDefaultLog());
    }
    static void setRequestTimeout(RaftProperties properties, TimeDuration timeoutDuration) {
      setTimeDuration(properties::setTimeDuration, REQUEST_TIMEOUT_KEY, timeoutDuration);
    }

    String WATCH_REQUEST_TIMEOUT_KEY = PREFIX + ".watch.request.timeout";
    TimeDuration WATCH_REQUEST_TIMEOUT_DEFAULT = TimeDuration.valueOf(20, TimeUnit.SECONDS);
    static TimeDuration watchRequestTimeout(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(WATCH_REQUEST_TIMEOUT_DEFAULT.getUnit()),
          WATCH_REQUEST_TIMEOUT_KEY, WATCH_REQUEST_TIMEOUT_DEFAULT, getDefaultLog());
    }
    static void setWatchRequestTimeout(RaftProperties properties,
        TimeDuration timeoutDuration) {
      setTimeDuration(properties::setTimeDuration, WATCH_REQUEST_TIMEOUT_KEY, timeoutDuration);
    }
  }

  interface Async {
    String PREFIX = RaftClientConfigKeys.PREFIX + ".async";

    String OUTSTANDING_REQUESTS_MAX_KEY = PREFIX + ".outstanding-requests.max";
    int OUTSTANDING_REQUESTS_MAX_DEFAULT = 100;
    static int outstandingRequestsMax(RaftProperties properties) {
      return getInt(properties::getInt, OUTSTANDING_REQUESTS_MAX_KEY,
          OUTSTANDING_REQUESTS_MAX_DEFAULT, getDefaultLog(), requireMin(2));
    }
    static void setOutstandingRequestsMax(RaftProperties properties, int outstandingRequests) {
      setInt(properties::setInt, OUTSTANDING_REQUESTS_MAX_KEY, outstandingRequests);
    }

    interface Experimental {
      String PREFIX = Async.PREFIX + "." + JavaUtils.getClassSimpleName(Experimental.class).toLowerCase();

      String SEND_DUMMY_REQUEST_KEY = PREFIX + ".send-dummy-request";
      boolean SEND_DUMMY_REQUEST_DEFAULT = true;
      static boolean sendDummyRequest(RaftProperties properties) {
        return getBoolean(properties::getBoolean, SEND_DUMMY_REQUEST_KEY, SEND_DUMMY_REQUEST_DEFAULT, getDefaultLog());
      }
      static void setSendDummyRequest(RaftProperties properties, boolean sendDummyRequest) {
        setBoolean(properties::setBoolean, SEND_DUMMY_REQUEST_KEY, sendDummyRequest);
      }
    }
  }

  interface DataStream {
    String PREFIX = RaftClientConfigKeys.PREFIX + ".data-stream";

    String OUTSTANDING_REQUESTS_MAX_KEY = PREFIX + ".outstanding-requests.max";
    int OUTSTANDING_REQUESTS_MAX_DEFAULT = 100;
    static int outstandingRequestsMax(RaftProperties properties) {
      return getInt(properties::getInt, OUTSTANDING_REQUESTS_MAX_KEY,
          OUTSTANDING_REQUESTS_MAX_DEFAULT, getDefaultLog(), requireMin(2));
    }
    static void setOutstandingRequestsMax(RaftProperties properties, int outstandingRequests) {
      setInt(properties::setInt, OUTSTANDING_REQUESTS_MAX_KEY, outstandingRequests);
    }

    String FLUSH_REQUEST_COUNT_MIN_KEY = PREFIX + ".flush.request.count.min";
    int FLUSH_REQUEST_COUNT_MIN_DEFAULT = 0;
    static int flushRequestCountMin(RaftProperties properties) {
      return getInt(properties::getInt, FLUSH_REQUEST_COUNT_MIN_KEY,
          FLUSH_REQUEST_COUNT_MIN_DEFAULT, getDefaultLog(), requireMin(0));
    }
    static void setFlushRequestCountMin(RaftProperties properties, int flushRequestCountMin) {
      setInt(properties::setInt, FLUSH_REQUEST_COUNT_MIN_KEY, flushRequestCountMin);
    }

    String FLUSH_REQUEST_BYTES_MIN_KEY = PREFIX + ".flush.request.bytes.min";
    SizeInBytes FLUSH_REQUEST_BYTES_MIN_DEFAULT = SizeInBytes.ONE_MB;
    static SizeInBytes flushRequestBytesMin(RaftProperties properties) {
      return getSizeInBytes(properties::getSizeInBytes, FLUSH_REQUEST_BYTES_MIN_KEY,
          FLUSH_REQUEST_BYTES_MIN_DEFAULT, getDefaultLog(), requireMinSizeInByte(SizeInBytes.ZERO));
    }
    static void setFlushRequestBytesMin(RaftProperties properties, SizeInBytes flushRequestBytesMin) {
      setSizeInBytes(properties::set, FLUSH_REQUEST_BYTES_MIN_KEY, flushRequestBytesMin);
    }

    String REQUEST_TIMEOUT_KEY = PREFIX + ".request.timeout";
    TimeDuration REQUEST_TIMEOUT_DEFAULT = TimeDuration.valueOf(10, TimeUnit.SECONDS);
    static TimeDuration requestTimeout(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(REQUEST_TIMEOUT_DEFAULT.getUnit()),
          REQUEST_TIMEOUT_KEY, REQUEST_TIMEOUT_DEFAULT, getDefaultLog());
    }
    static void setRequestTimeout(RaftProperties properties, TimeDuration timeoutDuration) {
      setTimeDuration(properties::setTimeDuration, REQUEST_TIMEOUT_KEY, timeoutDuration);
    }
  }

  interface MessageStream {
    String PREFIX = RaftClientConfigKeys.PREFIX + ".message-stream";

    String SUBMESSAGE_SIZE_KEY = PREFIX + ".submessage-size";
    SizeInBytes SUBMESSAGE_SIZE_DEFAULT = SizeInBytes.valueOf("1MB");
    static SizeInBytes submessageSize(RaftProperties properties) {
      return getSizeInBytes(properties::getSizeInBytes,
          SUBMESSAGE_SIZE_KEY, SUBMESSAGE_SIZE_DEFAULT, getDefaultLog());
    }
    static void setSubmessageSize(RaftProperties properties, SizeInBytes submessageSize) {
      setSizeInBytes(properties::set, SUBMESSAGE_SIZE_KEY, submessageSize, requireMin(SizeInBytes.ONE_KB));
    }
    static void setSubmessageSize(RaftProperties properties) {
      setSubmessageSize(properties, SUBMESSAGE_SIZE_DEFAULT);
    }
  }

  static void main(String[] args) {
    printAll(RaftClientConfigKeys.class);
  }
}
