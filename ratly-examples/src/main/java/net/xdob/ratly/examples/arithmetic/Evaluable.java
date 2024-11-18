package net.xdob.ratly.examples.arithmetic;

import java.util.Map;

public interface Evaluable {
  Double evaluate(Map<String, Double> variableMap);
}
