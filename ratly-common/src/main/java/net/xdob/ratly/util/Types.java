package net.xdob.ratly.util;

import com.google.common.collect.Maps;
import com.google.common.io.BaseEncoding;
import net.xdob.ratly.json.Size;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public abstract class Types {
  public static String DEFFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
  static final Logger LOG = LoggerFactory.getLogger(Types.class);

  public static String castToString(Object value) {
    if (value == null) {
      return null;
    }

    return value.toString();
  }

  public static Byte castToByte(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof Number) {
      return ((Number) value).byteValue();
    }

    if (value instanceof String) {
      String strVal = (String) value;
      if (strVal.isEmpty()) {
        return null;
      }
      return Byte.parseByte(strVal);
    }

    throw new CastException(byte.class, value);
  }

  public static Character castToChar(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof Character) {
      return (Character) value;
    }

    if (value instanceof String) {
      String strVal = (String) value;

      if (strVal.isEmpty()) {
        return null;
      }

      if (strVal.length() != 1) {
        throw new CastException(Character.class, value);
      }

      return strVal.charAt(0);
    }

    throw new CastException(Character.class, value);
  }

  public static Short castToShort(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof Number) {
      return ((Number) value).shortValue();
    }

    if (value instanceof String) {
      String strVal = (String) value;
      if (strVal.isEmpty()) {
        return null;
      }
      return Short.parseShort(strVal);
    }

    throw new CastException(short.class, value);
  }

  public static BigDecimal castToBigDecimal(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof BigDecimal) {
      return (BigDecimal) value;
    }

    if (value instanceof BigInteger) {
      return new BigDecimal((BigInteger) value);
    }

    String strVal = value.toString();
    if (strVal.isEmpty()) {
      return null;
    }

    return new BigDecimal(strVal);
  }

  public static BigInteger castToBigInteger(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof BigInteger) {
      return (BigInteger) value;
    }

    if (value instanceof Float || value instanceof Double) {
      return BigInteger.valueOf(((Number) value).longValue());
    }

    String strVal = value.toString();
    if (strVal.isEmpty()) {
      return null;
    }

    return new BigInteger(strVal);
  }

  public static Float castToFloat(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof Number) {
      return ((Number) value).floatValue();
    }

    if (value instanceof String) {
      String strVal = value.toString();
      if (strVal.isEmpty()) {
        return null;
      }

      return Float.parseFloat(strVal);
    }

    throw new CastException(float.class, value);
  }

  public static Double castToDouble(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }

    if (value instanceof String) {
      String strVal = value.toString();
      if (strVal.isEmpty()) {
        return null;
      }
      return Double.parseDouble(strVal);
    }

    throw new CastException(double.class, value);
  }

  public static Date castToDate(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof Calendar) {
      return ((Calendar) value).getTime();
    }

    if (value instanceof Date) {
      return (Date) value;
    }

    long longValue = -1;

    if (value instanceof Number) {
      longValue = ((Number) value).longValue();
    }

    if (value instanceof String) {
      String strVal = (String) value;

      if (strVal.indexOf('-') != -1) {
        String format;
        if (strVal.length() == DEFFAULT_DATE_FORMAT.length()) {
          format = DEFFAULT_DATE_FORMAT;
        } else if (strVal.length() == 10) {
          format = "yyyy-MM-dd";
        } else if (strVal.length() == "yyyy-MM-dd HH:mm:ss".length()) {
          format = "yyyy-MM-dd HH:mm:ss";
        } else {
          format = "yyyy-MM-dd HH:mm:ss.SSS";
        }

        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        try {
          return (Date) dateFormat.parse(strVal);
        } catch (ParseException e) {
          throw new CastException(Date.class, strVal);
        }
      }

      if (strVal.isEmpty()) {
        return null;
      }

      longValue = Long.parseLong(strVal);
    }

    if (longValue < 0) {
      throw new CastException(Date.class, value);
    }

    return new Date(longValue);
  }

  public static java.sql.Date castToSqlDate(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof Calendar) {
      return new java.sql.Date(((Calendar) value).getTimeInMillis());
    }

    if (value instanceof java.sql.Date) {
      return (java.sql.Date) value;
    }

    if (value instanceof Date) {
      return new java.sql.Date(((Date) value).getTime());
    }

    long longValue = 0;

    if (value instanceof Number) {
      longValue = ((Number) value).longValue();
    }

    if (value instanceof String) {
      String strVal = (String) value;
      if (strVal.isEmpty()) {
        return null;
      }

      longValue = Long.parseLong(strVal);
    }

    if (longValue <= 0) {
      throw new CastException(java.sql.Date.class, value);
    }

    return new java.sql.Date(longValue);
  }

  public static java.sql.Timestamp castToTimestamp(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof Calendar) {
      return new java.sql.Timestamp(((Calendar) value).getTimeInMillis());
    }

    if (value instanceof java.sql.Timestamp) {
      return (java.sql.Timestamp) value;
    }

    if (value instanceof Date) {
      return new java.sql.Timestamp(((Date) value).getTime());
    }

    long longValue = 0;

    if (value instanceof Number) {
      longValue = ((Number) value).longValue();
    }

    if (value instanceof String) {
      String strVal = (String) value;
      if (strVal.isEmpty()) {
        return null;
      }

      longValue = Long.parseLong(strVal);
    }

    if (longValue <= 0) {
      throw new CastException(java.sql.Timestamp.class, value);
    }

    return new java.sql.Timestamp(longValue);
  }

  public static Long castToLong(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof Number) {
      return ((Number) value).longValue();
    }

    if (value instanceof String) {
      String strVal = (String) value;
      if (strVal.isEmpty()) {
        return null;
      }

      try {
        return new BigDecimal(strVal.trim()).longValue();
      } catch (NumberFormatException ex) {
        //
      }

    }

    throw new CastException(Long.class, value);
  }

  public static Integer castToInt(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof Integer) {
      return (Integer) value;
    }

    if (value instanceof Number) {
      return ((Number) value).intValue();
    }

    if (value instanceof String) {
      String strVal = (String) value;
      if (strVal.isEmpty()) {
        return null;
      }

      return new BigDecimal(strVal.trim()).intValue();
    }

    throw new CastException(Integer.class, value);
  }

  public static byte[] castToBytes(Object value) {
    if (value instanceof byte[]) {
      return (byte[]) value;
    }

    if (value instanceof String) {
      return BaseEncoding.base64().decode((String) value);
    }
    throw new CastException(byte[].class, value);
  }

  public static Boolean castToBoolean(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof Boolean) {
      return (Boolean) value;
    }

    if (value instanceof Number) {
      return ((Number) value).intValue() == 1;
    }

    if (value instanceof String) {
      String str = (String) value;
      if (str.isEmpty()) {
        return null;
      }

      if ("true".equalsIgnoreCase(str)) {
        return Boolean.TRUE;
      }
      if ("false".equalsIgnoreCase(str)) {
        return Boolean.FALSE;
      }

      if ("yes".equalsIgnoreCase(str)) {
        return Boolean.TRUE;
      }
      if ("no".equalsIgnoreCase(str)) {
        return Boolean.FALSE;
      }
      if ("1".equals(str)) {
        return Boolean.TRUE;
      }
      if ("0".equals(str)) {
        return Boolean.FALSE;
      }
    }

    throw new CastException(Boolean.class, value);
  }
  
  @SuppressWarnings("unchecked")
  public static  <T> T castObject(Object source, Class<T> targetClass, NameMapping mapping) {
    if (source == null){
      return null;
    }
    T target = null;
    if(mapping==null){
      mapping = NameMapping.c();
    }
    if(source instanceof Map){
      target = mapToObject((Map<String, ?>)source, targetClass, mapping);
    }else if(targetClass == Map.class){
      target=(T)objectToMap(source, mapping);
    }else{
      try {
        target = targetClass.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        LOG.info("",e);
      }
      Copys.copy(source, target, mapping);
    }    
    return target;
  }

  static  <T> void castObject(Object source, T target, NameMapping mapping) {
    if(source instanceof Map){
      mapToObject((Map<?, ?>)source, target, mapping);
    }else if(target instanceof Map){
      objectToMap(source, mapping);
    }else{
      Copys.copy(source, target, mapping);
    }
  }

  static void objectToMap(Object source,Map<String, Object> target, NameMapping mapping) {
    Field[] fields = source.getClass().getDeclaredFields();
    for (Field field : fields) {
      int mod = field.getModifiers();
      if (Modifier.isStatic(mod) || Modifier.isFinal(mod)) {
        continue;
      }
      String name=mapping.getTarget(field.getName());
      field.setAccessible(true);
      try {
        Object value=field.get(source);
        if(value!=null){
          target.put(name, value);
        }
      } catch (IllegalArgumentException | IllegalAccessException e) {
        LOG.info("",e);
      }

    }
  }
  public static Map<String, Object> objectToMap(Object source, NameMapping mapping) {
    Map<String, Object> target = Maps.newHashMap();
      
    if(mapping==null){
      mapping=NameMapping.c(source.getClass()).inverse();
    }
    if(source instanceof Map){
      target.putAll((Map<String, ?>) source);
    }else {
      objectToMap(source, target, mapping);
    }
    return target;
  }

  @SuppressWarnings({"unchecked" })
  public static <T> T mapToObject(Map<String, ?> source, Class<T> targetClass, NameMapping mapping) {
    if (source == null){
      return null;
    }
      
    if(mapping==null){
      mapping=NameMapping.c(targetClass);
    }
    T target = null;
    try {
      target = targetClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      LOG.info("",e);
    }

    if(target instanceof Map){
      Map<String, Object> map = (Map<String, Object>) target;
      map.putAll(source);
    }else {
      mapToObject(source, target, mapping);
    }
    return target;
  }
  static List<Field> getFields(Class<?> clazz){
    Map<String,Field> fields = Maps.newHashMap();
    Class<?> tmpClass = clazz;
    while (tmpClass !=null && !tmpClass.equals(Object.class)) {
      final Field[] fields1 = tmpClass.getDeclaredFields();
      for (Field field : fields1) {
        fields.putIfAbsent(field.getName(),field);
      }
      tmpClass = tmpClass.getSuperclass();
    }
    return new ArrayList<>(fields.values());
  }

  static <T> void mapToObject(Map<?, ?> source, T target, NameMapping mapping) {
    if(target!=null){
      if(mapping==null){
        mapping=NameMapping.c();
      }
      List<Field> fields = getFields(target.getClass());
      for (Field field : fields) {
        int mod = field.getModifiers();
        if (Modifier.isStatic(mod)|| Modifier.isTransient(mod)) {
          continue;
        }
        if(Modifier.isFinal(mod)){
          if(Collection.class.isAssignableFrom(field.getType())){
            String name=mapping.getSource(field.getName());
            if (source.containsKey(name)) {
              field.setAccessible(true);
              try {
                Collection c = (Collection)field.get(target);
                c.clear();
                Type fc = field.getGenericType();
                if(fc instanceof ParameterizedType)
                {
                  ParameterizedType pt = (ParameterizedType) fc;
                  Class genericClazz = (Class)pt.getActualTypeArguments()[0];
                  Collection sc = (Collection)source.get(name);
                  for(Object o:sc){
                    c.add(cast(o, genericClazz));
                  }
                }

              } catch (IllegalArgumentException | IllegalAccessException e) {
                LOG.info("",e);
              }
            }
          }else if(Map.class.isAssignableFrom(field.getType())){
            String name=mapping.getSource(field.getName());
            if (source.containsKey(name)) {
              field.setAccessible(true);
              try {
                Map c = (Map)field.get(target);
                c.clear();
                Type fc = field.getGenericType();
                if(fc instanceof ParameterizedType)
                {
                  ParameterizedType pt = (ParameterizedType) fc;
                  Class genericClazz = (Class)pt.getActualTypeArguments()[1];
                  Map sc = (Map)source.get(name);
                  for(Object key:sc.keySet()){
                    Object o = sc.get(key);
                    c.put(key,cast(o, genericClazz));
                  }
                }
                c.putAll((Map)cast(source.get(name), field.getType()));
              } catch (IllegalArgumentException | IllegalAccessException e) {
                LOG.info("",e);
              }
            }
          }else{
            String name=mapping.getSource(field.getName());
            if (source.containsKey(name)) {
              final Object o1 = source.get(name);
              field.setAccessible(true);
              try {
                final Object o = field.get(target);
                castObject(o1,o,mapping);
              } catch (IllegalAccessException e) {
                LOG.info("",e);
              }
            }
          }
        }else{
          String name=mapping.getSource(field.getName());
          if (source.containsKey(name)) {
            field.setAccessible(true);
            try {
              Object value = cast(source.get(name), field.getType());
              if(field.getType().isPrimitive()) {
                if(value!=null){
                  field.set(target, value);
                }
              }else{
                field.set(target, value);
              }
            } catch (IllegalArgumentException | IllegalAccessException e) {
              LOG.info("",e);
            }
          }
        }
      }
    }
  }
  
  
  public static <T> T cast(Object obj, Class<T> clazz, T defValue) {
    T value = null;
    try
    {
      value = cast(obj,clazz);
    }catch(Exception ex){
      value = defValue;
    }
    return value;
  }
  
  public static <T> T cast(Object obj, Class<T> clazz){
    return cast(obj,clazz,(NameMapping)null);
  }
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static <T> T cast(Object obj, Class<T> clazz, NameMapping mapping) {
    if (obj == null) {
      return null;
    }

    if (clazz == null) {
      throw new IllegalArgumentException("clazz is null");
    }

    if (clazz == obj.getClass()) {
      return (T) obj;
    }

    if (obj instanceof Map) {
      if (clazz == Map.class) {
        return (T) obj;
      }
      if (N3Map.class.isAssignableFrom(clazz)) {
        try {
          N3Map m = (N3Map)clazz.newInstance();
          m.putAll((Map) obj);
          return (T) m;
        } catch (InstantiationException|IllegalAccessException  e) {
          throw new IllegalArgumentException("clazz newInstance fail.",e);
        }
      }
      if (clazz.equals(Object.class)) {
        return (T) obj;
      }
      return (T)mapToObject((Map)obj,clazz,mapping);
    }

    if (clazz.isArray()) {
      if (obj instanceof Collection) {

        Collection collection = (Collection) obj;
        int index = 0;
        Object array = Array.newInstance(clazz.getComponentType(), collection.size());
        for (Object item : collection) {
          Object value = cast(item, clazz.getComponentType());
          Array.set(array, index, value);
          index++;
        }

        return (T) array;
      }
      if(clazz.getComponentType().equals(byte.class)){
        if(obj instanceof String){
          return (T)BaseEncoding.base64().decode((String)obj);
        }
      }
    }

    if (clazz.isAssignableFrom(obj.getClass())) {
      return (T) obj;
    }

    if (clazz == boolean.class || clazz == Boolean.class) {
      return (T) castToBoolean(obj);
    }

    if (clazz == byte.class || clazz == Byte.class) {
      return (T) castToByte(obj);
    }

    // if (clazz == char.class || clazz == Character.class) {
    // return (T) castToCharacter(obj);
    // }

    if (clazz == short.class || clazz == Short.class) {
      return (T) castToShort(obj);
    }

    if (clazz == int.class || clazz == Integer.class) {
      return (T) castToInt(obj);
    }

    if (clazz == long.class || clazz == Long.class) {
      return (T) castToLong(obj);
    }

    if (clazz == float.class || clazz == Float.class) {
      return (T) castToFloat(obj);
    }

    if (clazz == double.class || clazz == Double.class) {
      return (T) castToDouble(obj);
    }

    if (clazz == String.class) {
      return (T) castToString(obj);
    }

    if (clazz == BigDecimal.class) {
      return (T) castToBigDecimal(obj);
    }

    if (clazz == BigInteger.class) {
      return (T) castToBigInteger(obj);
    }


    if(clazz == UUID.class){
      return (T) castToUUID(obj);
    }

    if (clazz == Date.class) {
      return (T) castToDate(obj);
    }

    if (clazz == java.sql.Date.class) {
      return (T) castToSqlDate(obj);
    }

    if (clazz == java.sql.Timestamp.class) {
      return (T) castToTimestamp(obj);
    }

    if (clazz.isEnum()) {
      return (T) castToEnum(obj, clazz);
    }

    if(clazz == Size.class){
      return (T) castSize(obj);
    }

    if (Calendar.class.isAssignableFrom(clazz)) {
      Date date = castToDate(obj);
      Calendar calendar;
      if (clazz == Calendar.class) {
        calendar = Calendar.getInstance();
      } else {
        try {
          calendar = (Calendar) clazz.newInstance();
        } catch (Exception e) {
          throw new CastException(e, clazz, obj);
        }
      }
      calendar.setTime(date);
      return (T) calendar;
    }

    if (obj instanceof String) {
      String strVal = (String) obj;
      if (strVal.isEmpty()) {
        return null;
      }
    }

    throw new CastException(clazz, obj);
  }

  private static Size castSize(Object obj) {
    return Size.parse(obj!=null?obj.toString():null);
  }


  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static <T> T castToEnum(Object obj, Class<T> clazz) {
    try {
      if (obj instanceof String) {
        String name = (String) obj;
        if (name.isEmpty()) {
          return null;
        }

        return (T) Enum.valueOf((Class<? extends Enum>) clazz, name);
      }

      if (obj instanceof Number) {
        int ordinal = ((Number) obj).intValue();

        Method method = clazz.getMethod("values");
        Object[] values = (Object[]) method.invoke(null);
        for (Object value : values) {
          Enum e = (Enum) value;
          if (e.ordinal() == ordinal) {
            return (T) e;
          }
        }
      }
    } catch (Exception ex) {
      throw new CastException(ex, clazz, obj);
    }

    throw new CastException(clazz, obj);
  }

  public static UUID castToUUID(Object value) {
    try {
      if (value == null) {
        return null;
      }

      if (value instanceof UUID) {
        return (UUID)value;
      }

      if (value instanceof String) {
        String name = (String) value;
        if (name.isEmpty()) {
          return null;
        }
        return UUID.fromString(name);
      }

      if (value instanceof byte[]) {
        byte[] name = (byte[]) value;
        return UUID.nameUUIDFromBytes(name);
      }
    } catch (Exception ex) {
      throw new CastException(ex, UUID.class, value);
    }
    throw new CastException(UUID.class, value);
  }

  @SuppressWarnings("unchecked")
  public static <T> T cast(Object obj, Type type) {
    if (obj == null) {
      return null;
    }

    if (type instanceof Class) {
      return (T) cast(obj, (Class<T>) type);
    }

    if (type instanceof ParameterizedType) {
      return (T) cast(obj, (ParameterizedType) type);
    }

    if (obj instanceof String) {
      String strVal = (String) obj;
      if (strVal.isEmpty()) {
        return null;
      }
    }

    if (type instanceof TypeVariable) {
      return (T) obj;
    }

    throw new CastException(type, obj);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static <T> T cast(Object obj, ParameterizedType type) {
    Type rawTye = type.getRawType();

    if (rawTye == List.class || rawTye == ArrayList.class) {
      Type itemType = type.getActualTypeArguments()[0];

      if (obj instanceof Iterable) {
        List list = new ArrayList();

        for (Object item : (Iterable) obj) {
          list.add(cast(item, itemType));
        }

        return (T) list;
      }
    }

    if (rawTye == Map.class || rawTye == HashMap.class) {
      Type keyType = type.getActualTypeArguments()[0];
      Type valueType = type.getActualTypeArguments()[1];

      if (obj instanceof Map) {
        Map map = new HashMap();

        for (Map.Entry entry : ((Map<?, ?>) obj).entrySet()) {
          Object key = cast(entry.getKey(), keyType);
          Object value = cast(entry.getValue(), valueType);

          map.put(key, value);
        }

        return (T) map;
      }
    }

    if (obj instanceof String) {
      String strVal = (String) obj;
      if (strVal.isEmpty()) {
        return null;
      }
    }

    if (type.getActualTypeArguments().length == 1) {
      Type argType = type.getActualTypeArguments()[0];
      if (argType instanceof WildcardType) {
        return (T) cast(obj, rawTye);
      }
    }

    throw new CastException(type, obj);
  }

}
