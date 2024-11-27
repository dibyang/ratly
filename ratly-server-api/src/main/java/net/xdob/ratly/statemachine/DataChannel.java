package net.xdob.ratly.statemachine;

import net.xdob.ratly.util.ReferenceCountedObject;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * 接口扩展了 WritableByteChannel，并添加了一个用于写入带引用计数的对象 (ReferenceCountedObject) 的方法。
 * 这个接口的设计旨在优化与内存管理相关的操作，尤其是在涉及引用计数（如直接缓冲区或内存池）时。
 */
public interface DataChannel extends WritableByteChannel {
  /**
   * 覆盖了 WritableByteChannel 的 write(ByteBuffer) 方法，但该方法默认抛出 UnsupportedOperationException。
   * 如果实现类已经覆盖了 write(ReferenceCountedObject) 方法，那么不需要再实现该方法。
   * This method is the same as {@link WritableByteChannel#write(ByteBuffer)}.
   * <p>
   * If the implementation has overridden {@link #write(ReferenceCountedObject)},
   * then it does not have to override this method.
   */
  @Override
  default int write(ByteBuffer buffer) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * 与 {@link #write(ByteBuffer)} 方法类似，但接受一个 {@link ReferenceCountedObject} 作为参数。
   * 这是一个包装了 ByteBuffer 的对象，具有引用计数功能。
   * 该方法的默认实现是通过 referenceCountedBuffer.get() 调用 write(ByteBuffer) 方法来实现写入操作。
   * 特殊说明：
   *    如果实现覆盖了此方法，它可以选择保留 (retain) 缓冲区，以便稍后使用。这种做法有助于优化内存使用，尤其是在涉及多次读取和写入的场景中。
   *    如果缓冲区被保留，它在不再使用时必须显式释放 (release)，否则会导致内存泄漏。
   *    如果缓冲区被保留多次，它必须被释放相同次数，否则也会引发资源泄漏。
   *    在方法返回之前，可以安全地访问缓冲区，不论是否保留。
   *    如果缓冲区没有被保留，但在此方法返回之后访问缓冲区，可能会导致数据不一致或损坏。
   *
   */
  default int write(ReferenceCountedObject<ByteBuffer> referenceCountedBuffer) throws IOException {
    return write(referenceCountedBuffer.get());
  }

  /**
   * 类似于 {@link java.nio.channels.FileChannel#force(boolean)} 方法，强制将数据和/或元数据写入底层存储设备，确保数据持久化。
   * 如果 metadata 为 true，表示需要强制写入元数据（如文件系统的文件属性、目录信息等）。
   *
   * @param metadata 是否强制写入元数据。
   * @throws IOException If there are IO errors.
   */
  void force(boolean metadata) throws IOException;
}
