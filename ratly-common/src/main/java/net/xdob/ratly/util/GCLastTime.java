package net.xdob.ratly.util;

import com.sun.management.GarbageCollectionNotificationInfo;

import javax.management.NotificationEmitter;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;

public class GCLastTime {
	private Timestamp lastGCTime = Timestamp.currentTime();

	public GCLastTime() {
		// 注册GC监听器
		registerGCListener();
	}

	private void registerGCListener() {
		List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
		for (GarbageCollectorMXBean gcBean : gcBeans) {
			NotificationEmitter emitter = (NotificationEmitter) gcBean;
			emitter.addNotificationListener((notification, handback) -> {
				if (notification.getType().equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION)) {
					lastGCTime = Timestamp.currentTime();
				}
			}, null, null);
		}
	}

	public Timestamp getLastGCTime() {
		return lastGCTime;
	}
}
