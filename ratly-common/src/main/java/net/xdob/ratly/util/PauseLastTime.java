package net.xdob.ratly.util;

public interface PauseLastTime {
	Timestamp getLastPauseTime();
	void start();
	void stop();
}
