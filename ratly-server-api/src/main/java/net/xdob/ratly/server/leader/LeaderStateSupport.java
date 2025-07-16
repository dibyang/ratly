package net.xdob.ratly.server.leader;

import java.util.Optional;

public interface LeaderStateSupport {
	Optional<LeaderState> getLeaderState();
}
