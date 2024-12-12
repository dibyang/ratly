package net.xdob.ratly.json;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;

/**
 * SpeedTypeAdapter for Speed
 * 
 * @author dib
 *
 */
public class SpeedTypeAdapter extends TypeAdapter<Speed> {
  public final static SpeedTypeAdapter SPEED = new SpeedTypeAdapter();
  public final static SpeedTypeAdapter CFG_SPEED = new SpeedTypeAdapter(false);

  private final boolean byteSize;

  private SpeedTypeAdapter() {
    this(true);
  }

  private SpeedTypeAdapter(boolean byteSize) {
    this.byteSize = byteSize;
  }

  @Override
  public void write(JsonWriter out, Speed value) throws IOException {
    if (byteSize) {
      if(value==null){
        out.nullValue();
      }else{
        out.value(value.getByteSpeed());
      }
    }else{
      out.value(value==null?"":value.toString());
    }
  }

  @Override
  public Speed read(JsonReader in) throws IOException {
    Speed value = null;
    if (byteSize) {
      if (in.peek() == JsonToken.NULL) {
        in.nextNull();
      }else{
        value = Speed.parse(in.nextString());
      }
    }else{
      String v = in.nextString();
      if(!v.isEmpty()){
        value = Speed.parse(v);
      }
    }
    return value;
  }

}
