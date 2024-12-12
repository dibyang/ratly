package net.xdob.ratly.jdbc.sql;


import com.google.protobuf.ByteString;
import net.xdob.ratly.proto.jdbc.ParamProto;
import net.xdob.ratly.fasts.serialization.FSTConfiguration;

import java.io.Serializable;


public class Parameter implements Serializable {
  private final int parameterIndex;
  private Object value;

  public Parameter(int parameterIndex) {
    this.parameterIndex = parameterIndex;
  }

  public int getParameterIndex() {
    return parameterIndex;
  }

  public Object getValue() {
    return value;
  }

  public Parameter setValue(Object value) {
    this.value = value;
    return this;
  }

  public ParamProto toParamProto(FSTConfiguration conf){
    ParamProto.Builder builder = ParamProto.newBuilder()
        .setIndex(parameterIndex);
    builder.setValue(ByteString.copyFrom(conf.asByteArray(value)));
    return builder.build();
  }

  public static Parameter c(int parameterIndex){
    return new Parameter(parameterIndex);
  }
}
