package net.xdob.ratly.json;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;

/**
 * TypeAdapter for Size
 * 
 * @author dib
 *
 */
public class SizeTypeAdapter extends TypeAdapter<Size> {
  public final static SizeTypeAdapter SIZE = new SizeTypeAdapter();
  public final static SizeTypeAdapter CFG_SIZE = new SizeTypeAdapter(false);

  private boolean byteSize;

  private SizeTypeAdapter() {
    this(true);
  }

  private SizeTypeAdapter(boolean byteSize) {
    this.byteSize = byteSize;
  }

  @Override
  public void write(JsonWriter out, Size value) throws IOException {
    if (byteSize) {
      if(value==null){
        out.nullValue();
      }else{
        out.value(value.getByteSize());
      }
    }else{
      out.value(value==null?"":value.toString());
    }
  }

  @Override
  public Size read(JsonReader in) throws IOException {
    Size value = null;
    if (byteSize) {
      if (in.peek() == JsonToken.NULL) {
        in.nextNull();
      }else{
        value = Size.parse(in.nextString());
      }
    }else{
      String v = in.nextString();
      if(!v.isEmpty()){
        value = Size.parse(v);
      }
    }
    return value;
  }

}
