package net.xdob.ratly.jdbc.sql;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import net.xdob.ratly.proto.jdbc.*;
import net.xdob.ratly.fasts.serialization.FSTConfiguration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.Function;

public class Parameters implements Serializable {
  private final ArrayList<Parameter> parameters = Lists.newArrayList();

  public Parameter computeIfAbsent(int parameterIndex, Function<Integer, Parameter> mappingFunction){

    synchronized (parameters) {
      Parameter parameter = parameters.stream().filter(e -> e.getParameterIndex() == parameterIndex)
          .findFirst().orElse(null);
      if(parameter==null){
        parameter= mappingFunction.apply(parameterIndex);
        parameters.add(parameter);
      }
      return parameter;
    }
  }

  public void clear(){
    parameters.clear();
  }

  public ParamListProto toParamListProto(FSTConfiguration conf){
    ParamListProto.Builder builder = ParamListProto.newBuilder();
    for (Parameter parameter : parameters) {
      builder.addParam(parameter.toParamProto(conf));
    }
    return builder.build();
  }

}
