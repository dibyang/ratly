package net.xdob.ratly.jdbc;

import net.xdob.ratly.statemachine.impl.*;

public class JdbcStateMachine extends CompoundStateMachine {

  public JdbcStateMachine(String db) {
    this.addSMPlugin(new DBSMPlugin(db));
  }

}
