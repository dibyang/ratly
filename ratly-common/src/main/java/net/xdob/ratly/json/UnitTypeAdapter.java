package net.xdob.ratly.json;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;

/**
 * 
 * 
 * TypeAdapter for Unit
 * 
 * @author dib
 *
 */
public class UnitTypeAdapter extends TypeAdapter<Unit> {
  public final static UnitTypeAdapter UNIT = new UnitTypeAdapter();

  @Override
  public void write(JsonWriter out, Unit value) throws IOException {
    out.value(value.toString());
  }

  @Override
  public Unit read(JsonReader in) throws IOException {
    // TODO Auto-generated method stub
    return Unit.parse(in.nextString());
  }

}
