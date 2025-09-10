package net.xdob.ratly.util;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class JvmPauseLastTime {
	private Timestamp lastJvmPauseTime = Timestamp.currentTime();
	private Timestamp lastCheckTime = Timestamp.currentTime();

	public JvmPauseLastTime(ScheduledExecutorService executor) {
		executor.scheduleAtFixedRate(this::checkJvmPause, 0, 100, TimeUnit.MILLISECONDS);
	}

	void checkJvmPause() {
		long elapsedTimeMs = lastCheckTime.elapsedTimeMs();
		//说明有JVM停顿
		if(elapsedTimeMs>160){
			lastJvmPauseTime = Timestamp.currentTime();
		}
		lastCheckTime = Timestamp.currentTime();
	}

	public Timestamp getLastJvmPauseTime() {
		return lastJvmPauseTime;
	}
}
