package net.xdob.ratly.util;

import com.sun.management.GarbageCollectionNotificationInfo;

import javax.management.ListenerNotFoundException;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;

public class GCLastTime implements PauseLastTime {
	private Timestamp lastGCTime = Timestamp.currentTime();
	private final NotificationListener notificationListener;

	public GCLastTime() {
		notificationListener = (notification, handback) -> {
			if (notification.getType().equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION)) {
				lastGCTime = Timestamp.currentTime();
			}
		};
	}

	private void registerGCListener() {

		List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
		for (GarbageCollectorMXBean gcBean : gcBeans) {
			NotificationEmitter emitter = (NotificationEmitter) gcBean;
			emitter.addNotificationListener(notificationListener, null, null);
		}
	}


	@Override
	public Timestamp getLastPauseTime() {
		return lastGCTime;
	}

	@Override
	public void start() {
		// 注册GC监听器
		registerGCListener();
	}

	@Override
	public void stop() {
		unregisterGCListener();
	}

	private void unregisterGCListener() {
		try {
			List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
			for (GarbageCollectorMXBean gcBean : gcBeans) {
				NotificationEmitter emitter = (NotificationEmitter) gcBean;
				emitter.removeNotificationListener(notificationListener);
			}
		} catch (ListenerNotFoundException ignore) {
		}
	}
}
