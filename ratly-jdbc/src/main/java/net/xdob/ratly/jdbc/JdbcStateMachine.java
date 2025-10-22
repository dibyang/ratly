package net.xdob.ratly.jdbc;

import net.xdob.ratly.statemachine.impl.CompoundStateMachine;


public class JdbcStateMachine extends CompoundStateMachine {

  public JdbcStateMachine() {
    this.addSMPlugin(new DBSMPlugin());
  }

  public JdbcStateMachine addDbIfAbsent(String db, String user, String password){
    getSMPlugin(DBSMPlugin.class)
        .ifPresent(e->e.addDbIfAbsent(db, user, password));
    return this;
  }

}
