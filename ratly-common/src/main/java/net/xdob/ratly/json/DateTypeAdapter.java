package net.xdob.ratly.json;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.util.Date;

/**
 * TypeAdapter for Date
 * 
 * @author dib
 *
 */
public class DateTypeAdapter extends TypeAdapter<Date> {
  public final static DateTypeAdapter DATE = new DateTypeAdapter();

  @Override
  public void write(JsonWriter out, Date value) throws IOException {
    if(value!=null){
      out.value(value.getTime());
    }else{
      out.nullValue();
    }
  }

  @Override
  public Date read(JsonReader in) throws IOException {
    Date value = null;
    if (in.peek() == JsonToken.NULL) {
      in.nextNull();
    }else{
      value = new Date(in.nextLong());
    }
    return value;
  }

}
