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

  public static void main(String[] args) throws InvalidProtocolBufferException {
    Parameters parameters = new Parameters();
    parameters.computeIfAbsent(1, Parameter::new).setValue("user3");
    parameters.computeIfAbsent(2, Parameter::new).setValue(null);
    FSTConfiguration fst = FSTConfiguration.createDefaultConfiguration();
    QueryRequestProto queryRequestProto = QueryRequestProto.newBuilder()
        .setSender(Sender.prepared)
        .setType(QueryType.query)
        .setParam(parameters.toParamListProto(fst))
        .build();
    byte[] bytes = queryRequestProto.toByteArray();
    FSTConfiguration fst2 = FSTConfiguration.createDefaultConfiguration();
    QueryRequestProto requestProto = QueryRequestProto.parseFrom(bytes);
    for (int i = 0; i < requestProto.getParam().getParamCount(); i++) {
      ParamProto param = requestProto.getParam().getParam(i);
      System.out.println("param.getIndex() = " + param.getIndex());
      System.out.println("param.getValue() = " + Arrays.toString(param.getValue().toByteArray()));
      System.out.println("2 param.getValue() = " + fst2.asObject(param.getValue().toByteArray()));
    }
  }
}
