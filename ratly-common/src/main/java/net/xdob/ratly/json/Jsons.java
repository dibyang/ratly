package net.xdob.ratly.json;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.ToNumberPolicy;

import java.io.Reader;
import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * Json 工具类
 * 
 * @author dib
 *
 */
public enum Jsons {
  i;
  final Gson gson;

  Jsons()
  {
    GsonBuilder builder = new GsonBuilder();
    builder
        //.setDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX")
        .registerTypeAdapter(Size.class, SizeTypeAdapter.SIZE)
        .registerTypeAdapter(Speed.class, SpeedTypeAdapter.SPEED)
        .registerTypeAdapter(Unit.class, UnitTypeAdapter.UNIT)
        .registerTypeAdapter(Date.class, DateTypeAdapter.DATE)
        .registerTypeAdapter(Timestamp.class, DateTypeAdapter.DATE)
        .registerTypeAdapter(java.sql.Date.class, DateTypeAdapter.DATE)
        .setObjectToNumberStrategy(ToNumberPolicy.BIG_DECIMAL)
        .setNumberToNumberStrategy(ToNumberPolicy.BIG_DECIMAL)
        .setExclusionStrategies(new ThrowableExclusionStrategy())
        .setPrettyPrinting().serializeNulls().disableHtmlEscaping();
    ServiceLoader<GsonBuild> serviceLoader = ServiceLoader.load(GsonBuild.class);
    for (GsonBuild gsonBuild : serviceLoader) {
      gsonBuild.build(builder);
    }
    gson = builder.create();
  }

  public Gson getGson(){
    return gson;
  }
  
  public String toJson(Object src) {
    return gson.toJson(src);
  }

  public void toJson(Object src, Appendable writer) {
    gson.toJson(src, writer);
  }

  public void toJson(Object src, Type typeOfSrc, Appendable writer) {
    gson.toJson(src, typeOfSrc, writer);
  }

  public <T> T fromJson(String json, Class<T> classOfT) {
    return gson.fromJson(json, classOfT);
  }

  public <T> T fromJson(String json, Type typeOfT) {
    return gson.fromJson(json, typeOfT);
  }

  public <T> T fromJson(Reader reader, Type typeOfT) {
    return gson.fromJson(reader, typeOfT);
  }

  public <T> T fromJson(Reader reader, Class<T> classOfT) {
    return gson.fromJson(reader, classOfT);
  }
  
  public <T> T fromJson(JsonElement reader, Type typeOfT) {
    return gson.fromJson(reader, typeOfT);
  }

  public <T> T fromJson(JsonElement reader, Class<T> classOfT) {
    return gson.fromJson(reader, classOfT);
  }
  
  public static void main(String[] args) {
    System.out.println(Jsons.i.toJson(new Exception(new Exception("pp"))));
  }

}
