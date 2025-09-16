package net.xdob.ratly.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ThreadFactory;

public interface ThreadUtil {
	static ThreadFactory getThreadFactory(String nameFormat, boolean daemon) {
		return new ThreadFactoryBuilder()
				.setDaemon(daemon)
				.setNameFormat(nameFormat)
				.build();
	}
}
