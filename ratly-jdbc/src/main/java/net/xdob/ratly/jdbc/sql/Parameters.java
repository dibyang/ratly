package net.xdob.ratly.jdbc.sql;

import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class Parameters implements Serializable {
  private final ArrayList<Parameter> parameters = Lists.newArrayList();

  public Parameter computeIfAbsent(int parameterIndex, Function<Integer, Parameter> mappingFunction){

    synchronized (parameters) {
      Parameter parameter = parameters.stream().filter(e -> e.getIndex() == parameterIndex)
          .findFirst().orElse(null);
      if(parameter==null){
        parameter= mappingFunction.apply(parameterIndex);
        parameters.add(parameter);
      }
      return parameter;
    }
  }

  public Parameters clear(){
    parameters.clear();
    return this;
  }

  public Parameters addAll(Parameters parameters){
    this.parameters.addAll(parameters.parameters);
    return this;
  }

  public List<Parameter> getParameters() {
    return parameters;
  }

  public boolean isEmpty(){
    return parameters.isEmpty();
  }
}
