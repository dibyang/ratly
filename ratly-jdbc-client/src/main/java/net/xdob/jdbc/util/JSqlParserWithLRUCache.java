package net.xdob.jdbc.util;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.Statement;
import net.xdob.ratly.util.Proto2Util;
import net.xdob.ratly.util.ProtoUtils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public interface JSqlParserWithLRUCache {
	// 使用LinkedHashMap实现LRU缓存
	class LRUCache<K, V> extends LinkedHashMap<K, V> {
		private final int maxSize;

		public LRUCache(int maxSize) {
			// 设置accessOrder为true，使得LinkedHashMap按访问顺序排序
			super(16, 0.75f, true);
			this.maxSize = maxSize;
		}

		@Override
		protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
			// 当缓存大小超过最大值时，移除最久未使用的条目
			return size() > maxSize;
		}
	}

	// 线程安全的LRU缓存
	LRUCache<String, Statement> STATEMENT_CACHE = new LRUCache<>(1024);
	ReadWriteLock LOCK = new ReentrantReadWriteLock();

	/**
	 * 解析SQL并缓存结果
	 * @param sql 要解析的SQL语句
	 * @return 解析后的Statement对象
	 * @throws JSQLParserException 如果解析失败
	 */
	static Statement parse(String sql) throws JSQLParserException {
		// 先尝试从缓存中获取（读锁）
		LOCK.readLock().lock();
		try {
			Statement cached = STATEMENT_CACHE.get(sql);
			if (cached != null) {
				return cached;
			}
		} finally {
			LOCK.readLock().unlock();
		}

		// 缓存中没有，进行解析
		Statement statement = CustomSqlParser.parse(sql);

		// 将解析结果放入缓存（写锁）
		LOCK.writeLock().lock();
		try {
			STATEMENT_CACHE.put(sql, statement);
			return statement;
		} finally {
			LOCK.writeLock().unlock();
		}
	}

	static Statement clone(Statement cached) {
		return (Statement) Proto2Util.toObject(Proto2Util.writeObject2ByteString(cached));
	}


	/**
	 * 清空缓存
	 */
	public static void clearCache() {
		LOCK.writeLock().lock();
		try {
			STATEMENT_CACHE.clear();
		} finally {
			LOCK.writeLock().unlock();
		}
	}

	/**
	 * 获取当前缓存大小
	 */
	static int getCacheSize() {
		LOCK.readLock().lock();
		try {
			return STATEMENT_CACHE.size();
		} finally {
			LOCK.readLock().unlock();
		}
	}
}
