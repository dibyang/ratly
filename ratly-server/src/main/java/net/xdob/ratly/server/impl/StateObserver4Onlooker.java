package net.xdob.ratly.server.impl;

import net.xdob.onlooker.DefaultOnlookerClient;
import net.xdob.onlooker.MessageToken;
import net.xdob.onlooker.OnlookerClient;
import net.xdob.ratly.security.SignHelper;
import net.xdob.ratly.server.StateObserver;
import net.xdob.ratly.server.TermLeader;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class StateObserver4Onlooker implements StateObserver {
  private final SignHelper signHelper = new SignHelper();
  private final OnlookerClient onlookerClient = new DefaultOnlookerClient();

  @Override
  public void start() {
    onlookerClient.start();
  }

  @Override
  public void notifyTeamIndex(String groupId, TermLeader termLeader) {
    MessageToken token = new MessageToken();
    token.setSigner(signHelper.getSigner());
    String leaderId = termLeader.getLeaderId();
    token.setMessage(leaderId);
    token.setTeam(termLeader.getTerm());
    token.setIndex(termLeader.getIndex());
    String key = termLeader.getLeaderId() +
        "$" +
        termLeader.getTerm() +
        "_" +
        termLeader.getIndex();
    token.setSign(signHelper.sign(key));
    onlookerClient.setMessage(groupId, token);
  }

  @Override
  public CompletableFuture<TermLeader> getLastLeaderTerm(String groupId, int waitMS) {
    final CompletableFuture<TermLeader> future = new CompletableFuture<>();
    onlookerClient.getMessageToken(groupId.toString(), waitMS)
        .whenComplete((r,ex)->{
          if(ex!=null){
            future.completeExceptionally(ex);
          }else{
            List<TermLeader> termLeaders = r.stream()
                .filter(e -> signHelper.verifySign(e.getMessage() +
                    "$" +
                    e.getTerm() +
                    "_" +
                    e.getIndex(), e.getSign()))
                .map(m -> {
                  TermLeader leader = TermLeader.parse(m.getMessage());
                  leader.setIndex(m.getIndex());
                  return leader;
                }).collect(Collectors.toList());
            long term = termLeaders.stream().mapToLong(TermLeader::getTerm).max().orElse(-1L);
            TermLeader termLeader = termLeaders.stream().filter(e->e.getTerm()== term)
                .max(Comparator.comparingLong(TermLeader::getIndex)).orElse(null);
            //LOG.info("tokens={}, termLeader={}", r, termLeader);
            future.complete(termLeader);
          }
        });
    return  future;
  }

  @Override
  public void close() throws IOException {
    onlookerClient.stop();
  }
}
