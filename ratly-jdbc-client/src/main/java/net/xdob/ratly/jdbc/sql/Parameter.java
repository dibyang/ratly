package net.xdob.ratly.jdbc.sql;


import net.xdob.ratly.protocol.Value;
import net.xdob.ratly.proto.jdbc.ParameterProto;

import java.io.Serializable;
import java.sql.SQLException;


public class Parameter implements Serializable {
  private final int index;
  private Object value;

  public Parameter(int index) {
    this.index = index;
  }

  public int getIndex() {
    return index;
  }

  public Object getValue() {
    return value;
  }

  public Parameter setValue(Object value) {
    this.value = value;
    return this;
  }

  public ParameterProto toProto() throws SQLException {
    return toProto(this);
  }

  public static Parameter from(ParameterProto parameterProto) throws SQLException {
    return new Parameter(parameterProto.getIndex())
        .setValue(Value.toJavaObject(parameterProto.getValue()));
  }

  public static ParameterProto toProto(Parameter parameter) throws SQLException {
    return ParameterProto.newBuilder()
        .setIndex(parameter.getIndex())
        .setValue(Value.toValueProto(parameter.getValue()))
        .build();
  }

  public static Parameter c(int parameterIndex){
    return new Parameter(parameterIndex);
  }
}
