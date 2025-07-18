package net.xdob.ratly.jdbc.sql;

import com.google.common.collect.Maps;

import java.io.Serializable;
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
}
