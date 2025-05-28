package net.xdob.ratly.server.storage;

@FunctionalInterface
public interface StorageChecker {
	boolean check(RaftStorage storage);

	class NOOP  implements StorageChecker {
		@Override
		public boolean check(RaftStorage storage) {
			return true;
		}
	}

}
