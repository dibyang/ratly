package net.xdob.ratly.server.storage;

import java.io.IOException;

/**
 * Represent a file on disk which persistently stores the metadata of a raft storage.
 * The file is updated atomically.
 */
public interface RaftStorageMetadataFile {
  /** @return the metadata persisted in this file. */
  RaftStorageMetadata getMetadata() throws IOException;

  /** Persist the given metadata. */
  void persist(RaftStorageMetadata newMetadata) throws IOException;
}
