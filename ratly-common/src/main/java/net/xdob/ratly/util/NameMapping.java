package net.xdob.ratly.util;


import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.gson.annotations.SerializedName;

import java.lang.reflect.Field;
import java.util.List;

public class NameMapping {
  private final BiMap<String, String> mapping = HashBiMap.create();

  public NameMapping map(String source, String target) {
    mapping.put(source, target);
    return this;
  }

  public NameMapping remove(String source) {
    mapping.remove(source);
    return this;
  }

  public NameMapping clear() {
    mapping.clear();
    return this;
  }

  public String getTarget(String source) {
    String target = mapping.get(source);
    if (target == null) {
      target = source;
    }
    return target;
  }
  
  public String getSource(String target) {
    String source = mapping.inverse().get(target);
    if (source == null) {
      source = target;
    }
    return source;
  }

  public NameMapping inverse(){
    final NameMapping nameMapping = new NameMapping();
    nameMapping.mapping.putAll(this.mapping.inverse());
    return nameMapping;
  }

  public static NameMapping c() {
    return new NameMapping();
  }

  public static NameMapping c(Class clazz) {
    final NameMapping nameMapping = new NameMapping();
    final List<Field> fields = Types.getFields(clazz);
    for (Field field : fields) {
      SerializedName serializedName = field.getAnnotation(SerializedName.class);
      if(serializedName!=null){
        nameMapping.map(serializedName.value(),field.getName());
      }
    }
    return nameMapping;
  }

}
