package net.xdob.jdbc.sql;

import com.google.common.collect.Maps;
import net.xdob.ratly.protocol.Value;
import net.xdob.ratly.proto.jdbc.ParameterProto;
import net.xdob.ratly.proto.jdbc.ParametersProto;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Parameters implements Serializable {
  private final Map<Integer,Parameter> parameters = Maps.newConcurrentMap();

  public Parameter getOrCreate(int parameterIndex){
    return parameters.computeIfAbsent(parameterIndex, Parameter::c);
  }

  public Parameters clear(){
    parameters.clear();
    return this;
  }

  public Parameters addAll(Parameters parameters){
    this.parameters.putAll(parameters.parameters);
    return this;
  }

  public List<Parameter> getParameters() {
    return new ArrayList<>(parameters.values());
  }

  public boolean isEmpty(){
    return parameters.isEmpty();
  }

  public ParametersProto toProto() throws SQLException {
    return toProto(this);
  }

  public static Parameters from(ParametersProto parametersProto) throws SQLException {
    Parameters parameters = new Parameters();
    for (ParameterProto proto : parametersProto.getParamList()) {
      parameters.getOrCreate(proto.getIndex())
          .setValue(Value.toJavaObject(proto.getValue()));
    }
    return parameters;
  }

  public static ParametersProto toProto(Parameters parameters) throws SQLException {
    ParametersProto.Builder builder = ParametersProto.newBuilder();
    for (Parameter parameter : parameters.getParameters()) {
      builder.addParam(Parameter.toProto(parameter));
    }
    return builder.build();
  }
}
