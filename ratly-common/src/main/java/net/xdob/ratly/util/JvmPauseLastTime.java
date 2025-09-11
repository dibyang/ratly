package net.xdob.ratly.util;

import org.slf4j.Logger;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class JvmPauseLastTime implements PauseLastTime {

	private Logger logger;
	private Timestamp lastJvmPauseTime = Timestamp.currentTime();
	private Timestamp lastCheckTime = Timestamp.currentTime();
	private int period = 100;
	private ScheduledFuture<?> future;
	private ScheduledExecutorService executor;
	public JvmPauseLastTime(ScheduledExecutorService executor) {
		this.executor = executor;
	}

	public JvmPauseLastTime setLogger(Logger logger) {
		this.logger = logger;
		return this;
	}

	public void start() {
		future = executor.scheduleWithFixedDelay(this::checkJvmPause, 0, period, TimeUnit.MILLISECONDS);
	}

	public void stop() {
		if(future!=null){
			future.cancel(true);
		}
	}

	void checkJvmPause() {
		long pauseTime = lastCheckTime.elapsedTimeMs()- period;
		//说明有JVM停顿
		if(pauseTime>=10){
			lastJvmPauseTime = Timestamp.currentTime();
			if(logger!=null){
				logger.info("JVM pause {} ms", pauseTime);
			}
		}
		lastCheckTime = Timestamp.currentTime();
	}


	@Override
	public Timestamp getLastPauseTime() {
		return lastJvmPauseTime;
	}
}
