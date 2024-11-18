package net.xdob.ratly.examples.filestore.cli;

import com.beust.jcommander.Parameter;
import net.xdob.ratly.RaftConfigKeys;
import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.client.RaftClientConfigKeys;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.datastream.SupportedDataStreamType;
import net.xdob.ratly.examples.common.SubCommandBase;
import net.xdob.ratly.examples.filestore.FileStoreClient;
import net.xdob.ratly.grpc.GrpcConfigKeys;
import net.xdob.ratly.grpc.GrpcFactory;
import net.xdob.ratly.protocol.ClientId;
import net.xdob.ratly.protocol.RaftGroup;
import net.xdob.ratly.protocol.RaftGroupId;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.rpc.SupportedRpcType;
import net.xdob.ratly.server.RaftServerConfigKeys;
import com.google.protobuf.ByteString;
import net.xdob.ratly.util.FileUtils;
import net.xdob.ratly.util.SizeInBytes;
import net.xdob.ratly.util.TimeDuration;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Client to connect filestore example cluster.
 */
public abstract class Client extends SubCommandBase {

  @Parameter(names = {"--size"}, description = "Size of each file in bytes", required = true)
  private long fileSizeInBytes;

  @Parameter(names = {"--bufferSize"}, description = "Size of buffer in bytes, should less than 4MB, " +
      "i.e BUFFER_BYTE_LIMIT_DEFAULT", required = false)
  private int bufferSizeInBytes = 1024;

  @Parameter(names = {"--numFiles"}, description = "Number of files to be written", required = true)
  private int numFiles;

  @Parameter(names = {"--numClients"}, description = "Number of clients to write", required = true)
  private int numClients;

  @Parameter(names = {"--storage", "-s"}, description = "Storage dir, eg. --storage dir1 --storage dir2",
      required = true)
  private List<File> storageDir = new ArrayList<>();

  private static final int MAX_THREADS_NUM = 1000;

  public int getNumThread() {
    return numFiles < MAX_THREADS_NUM ? numFiles : MAX_THREADS_NUM;
  }

  public long getFileSizeInBytes() {
    return fileSizeInBytes;
  }

  public int getBufferSizeInBytes() {
    return bufferSizeInBytes;
  }

  public int getNumFiles() {
    return numFiles;
  }

  @Override
  public void run() throws Exception {
    int raftSegmentPreallocatedSize = 1024 * 1024 * 1024;
    RaftProperties raftProperties = new RaftProperties();
    RaftConfigKeys.Rpc.setType(raftProperties, SupportedRpcType.GRPC);
    GrpcConfigKeys.setMessageSizeMax(raftProperties,
        SizeInBytes.valueOf(raftSegmentPreallocatedSize));
    RaftServerConfigKeys.Log.Appender.setBufferByteLimit(raftProperties,
        SizeInBytes.valueOf(raftSegmentPreallocatedSize));
    RaftServerConfigKeys.Log.setWriteBufferSize(raftProperties,
        SizeInBytes.valueOf(raftSegmentPreallocatedSize));
    RaftServerConfigKeys.Log.setPreallocatedSize(raftProperties,
        SizeInBytes.valueOf(raftSegmentPreallocatedSize));
    RaftServerConfigKeys.Log.setSegmentSizeMax(raftProperties,
        SizeInBytes.valueOf(1 * 1024 * 1024 * 1024L));
    RaftConfigKeys.DataStream.setType(raftProperties, SupportedDataStreamType.NETTY);

    RaftServerConfigKeys.Log.setSegmentCacheNumMax(raftProperties, 2);

    RaftClientConfigKeys.Rpc.setRequestTimeout(raftProperties,
        TimeDuration.valueOf(50000, TimeUnit.MILLISECONDS));
    RaftClientConfigKeys.Async.setOutstandingRequestsMax(raftProperties, 1000);

    for (File dir : storageDir) {
      FileUtils.createDirectories(dir);
    }

    operation(getClients(raftProperties));
  }

  public List<FileStoreClient> getClients(RaftProperties raftProperties) {
    List<FileStoreClient> fileStoreClients = new ArrayList<>();
    for (int i = 0; i < numClients; i ++) {
      final RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(ByteString.copyFromUtf8(getRaftGroupId())),
          getPeers());

      RaftClient.Builder builder =
          RaftClient.newBuilder().setProperties(raftProperties);
      builder.setRaftGroup(raftGroup);
      builder.setClientRpc(
          new GrpcFactory(new net.xdob.ratly.conf.Parameters())
              .newRaftClientRpc(ClientId.randomId(), raftProperties));
      RaftPeer[] peers = getPeers();
      builder.setPrimaryDataStreamServer(peers[0]);
      RaftClient client = builder.build();
      fileStoreClients.add(new FileStoreClient(client));
    }
    return fileStoreClients;
  }


  protected void close(List<FileStoreClient> clients) throws IOException {
    for (FileStoreClient client : clients) {
      client.close();
    }
  }

  public String getPath(String fileName) {
    int hash = fileName.hashCode() % storageDir.size();
    return new File(storageDir.get(Math.abs(hash)), fileName).getAbsolutePath();
  }

  protected void dropCache() {
    String[] cmds = {"/bin/sh","-c","echo 3 > /proc/sys/vm/drop_caches"};
    try {
      Process pro = Runtime.getRuntime().exec(cmds);
      pro.waitFor();
    } catch (Throwable t) {
      if (t instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      System.err.println("Failed to run command:" + Arrays.toString(cmds) + ":" + t.getMessage());
    }
  }

  private CompletableFuture<Long> writeFileAsync(String path, ExecutorService executor) {
    final CompletableFuture<Long> future = new CompletableFuture<>();
    CompletableFuture.supplyAsync(() -> {
      try {
        future.complete(writeFile(path, fileSizeInBytes, bufferSizeInBytes));
      } catch (IOException e) {
        future.completeExceptionally(e);
      }
      return future;
    }, executor);
    return future;
  }

  protected List<String> generateFiles(ExecutorService executor) {
    UUID uuid = UUID.randomUUID();
    List<String> paths = new ArrayList<>();
    List<CompletableFuture<Long>> futures = new ArrayList<>();
    for (int i = 0; i < numFiles; i ++) {
      String path = getPath("file-" + uuid + "-" + i);
      paths.add(path);
      futures.add(writeFileAsync(path, executor));
    }

    for (int i = 0; i < futures.size(); i ++) {
      long size = futures.get(i).join();
      if (size != fileSizeInBytes) {
        System.err.println("Error: path:" + paths.get(i) + " write:" + size +
            " mismatch expected size:" + fileSizeInBytes);
      }
    }

    return paths;
  }

  protected long writeFile(String path, long fileSize, long bufferSize) throws IOException {
    final byte[] buffer = new byte[Math.toIntExact(bufferSize)];
    long offset = 0;
    try(RandomAccessFile raf = new RandomAccessFile(path, "rw")) {
      while (offset < fileSize) {
        final long remaining = fileSize - offset;
        final long chunkSize = Math.min(remaining, bufferSize);
        ThreadLocalRandom.current().nextBytes(buffer);
        raf.write(buffer, 0, Math.toIntExact(chunkSize));
        offset += chunkSize;
      }
    }
    return offset;
  }

  protected abstract void operation(List<FileStoreClient> clients)
      throws IOException, ExecutionException, InterruptedException;
}
