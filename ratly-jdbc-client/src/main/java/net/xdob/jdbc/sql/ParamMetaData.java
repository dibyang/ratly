package net.xdob.jdbc.sql;

import net.xdob.ratly.proto.jdbc.ParameterMetaProto;

import java.io.Serializable;

public class ParamMetaData implements Serializable {
  private int nullable;
  private int precision;
  private int scale;
  private int parameterMode;
  private int parameterType;
  private String parameterTypeName;
  private String parameterClassName;

  public int isNullable() {
    return nullable;
  }

  public ParamMetaData setNullable(int nullable) {
    this.nullable = nullable;
    return this;
  }

  public int getPrecision() {
    return precision;
  }

  public ParamMetaData setPrecision(int precision) {
    this.precision = precision;
    return this;
  }

  public int getScale() {
    return scale;
  }

  public ParamMetaData setScale(int scale) {
    this.scale = scale;
    return this;
  }

  public int getParameterMode() {
    return parameterMode;
  }

  public ParamMetaData setParameterMode(int parameterMode) {
    this.parameterMode = parameterMode;
    return this;
  }

  public int getParameterType() {
    return parameterType;
  }

  public ParamMetaData setParameterType(int parameterType) {
    this.parameterType = parameterType;
    return this;
  }

  public String getParameterTypeName() {
    return parameterTypeName;
  }

  public ParamMetaData setParameterTypeName(String parameterTypeName) {
    this.parameterTypeName = parameterTypeName;
    return this;
  }

  public String getParameterClassName() {
    return parameterClassName;
  }

  public ParamMetaData setParameterClassName(String parameterClassName) {
    this.parameterClassName = parameterClassName;
    return this;
  }

  public ParameterMetaProto.Parameter toProto(){
    return toProto(this);
  }

  public static ParamMetaData from(ParameterMetaProto.Parameter proto){
    ParamMetaData metaData = new ParamMetaData();
    metaData.setNullable(proto.getNullable())
        .setParameterClassName(proto.getParameterClassName())
        .setPrecision(proto.getPrecision())
        .setScale(proto.getScale())
        .setParameterMode(proto.getParameterMode())
        .setParameterType(proto.getParameterType())
        .setParameterTypeName(proto.getParameterTypeName());
    return metaData;
  }

  public static ParameterMetaProto.Parameter toProto(ParamMetaData metaData){
    return ParameterMetaProto.Parameter.newBuilder()
        .setNullable(metaData.isNullable())
        .setParameterClassName(metaData.getParameterClassName())
        .setPrecision(metaData.getPrecision())
        .setScale(metaData.getScale())
        .setParameterMode(metaData.getParameterMode())
        .setParameterType(metaData.getParameterType())
        .setParameterTypeName(metaData.getParameterTypeName())
        .build();
  }
}
