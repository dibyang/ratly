package net.xdob.ratly.jdbc.sql;


import java.io.Serializable;
import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SerialParameterMetaData implements ParameterMetaData, Serializable {
  private final List<ParamMetaData> parameters = new ArrayList<>();


  public SerialParameterMetaData(ParameterMetaData metaData) throws SQLException {
    fill(metaData);
  }

  public SerialParameterMetaData fill(ParameterMetaData metaData) throws SQLException {
    int parameterCount = metaData.getParameterCount();
    for (int param = 1; param <= parameterCount; param++) {
      ParamMetaData pmd = new ParamMetaData();
      pmd.setPrecision(metaData.getPrecision(param));
      pmd.setScale(metaData.getScale(param));
      pmd.setNullable(metaData.isNullable(param));
      pmd.setParameterType(metaData.getParameterType(param));
      pmd.setParameterTypeName(metaData.getParameterTypeName(param));
      pmd.setParameterClassName(metaData.getParameterClassName(param));
      pmd.setParameterMode(metaData.getParameterMode(param));
      parameters.add(pmd);
    }
    return this;
  }

  @Override
  public int getParameterCount() throws SQLException {
    return parameters.size();
  }

  private ParamMetaData getParameter(int param) throws SQLException {
    if (param < 1 || param > parameters.size()) {
      throw new SQLException("param index error:"+param);
    }
    return parameters.get(param - 1);
  }

  @Override
  public int isNullable(int param) throws SQLException {
    return getParameter(param).isNullable();
  }

  @Override
  public boolean isSigned(int param) throws SQLException {
    getParameter(param);
    return true;
  }

  @Override
  public int getPrecision(int param) throws SQLException {
    return getParameter(param).getPrecision();
  }

  @Override
  public int getScale(int param) throws SQLException {
    return getParameter(param).getScale();
  }

  @Override
  public int getParameterType(int param) throws SQLException {
    return getParameter(param).getParameterType();
  }

  @Override
  public String getParameterTypeName(int param) throws SQLException {
    return getParameter(param).getParameterTypeName();
  }

  @Override
  public String getParameterClassName(int param) throws SQLException {
    return getParameter(param).getParameterClassName();
  }

  @Override
  public int getParameterMode(int param) throws SQLException {
    return getParameter(param).getParameterMode();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    try {
      if (isWrapperFor(iface)) {
        return (T) this;
      }
      throw new SQLException("iface invalid :"+iface);
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface != null && iface.isAssignableFrom(getClass());
  }
}
