package net.xdob.ratly.json;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 速度/带宽对象
 * 
 * @author 杨志坚 Email: dib.yang@gmail.com
 *
 */
public class Speed {
  private final static Pattern pattern = Pattern
      .compile("((\\d*\\.)?\\d+(([eE])\\d+)?\\s*)(B|BYTES|K|KB|M|MB|G|GB|T|TB|P|PB|E|EB|Z|ZB)?\\/S");

  private final static DecimalFormat df = new DecimalFormat("0.###",
      DecimalFormatSymbols.getInstance(Locale.US));

  private long byteSpeed = 0;
  private Unit unit = Unit.MB;

  public void applyPattern(String pattern) {
    df.applyPattern(pattern);
  }

  public long getByteSpeed() {
    return byteSpeed;
  }

  public void setByteSpeed(long byteSpeed) {
    this.unit = Unit.machUnit(byteSpeed);
    this.byteSpeed = byteSpeed;
  }

  public Speed(long byteSpeed) {
    if (byteSpeed < 0)
      throw new IllegalArgumentException("byteSpeed < 0");
    this.setByteSpeed(byteSpeed);
  }
  
  public Speed(double byteSpeed) {
    this((long)byteSpeed);
  }

  public Speed(double speed, Unit unit) {
    this.setUnit(unit);
    this.setSpeed(speed);
  }

  public Unit getUnit() {
    if (unit == null)
      unit = Unit.bytes;
    return this.unit;
  }

  public double getSpeed() {
    return this.unit.machSize(byteSpeed);
  }

  public void setSpeed(double speed) {
    this.byteSpeed = (long)(speed*this.unit.getRate());
  }

  public void setUnit(Unit unit) {
    if (unit != null) {
      this.unit = unit;
    } else {
      this.unit = Unit.bytes;
    }
  }

  @Override
  public boolean equals(Object obj) {
    // TODO Auto-generated method stub
    if (obj instanceof Speed) {
      return this.byteSpeed == ((Speed) obj).byteSpeed;
    }
    return false;
  }

  public Speed add(Speed speed) {
    return Speed.toSpeed(this.getByteSpeed() + speed.getByteSpeed());
  }

  public Speed subtract(Speed speed) {
    return Speed.toSpeed(this.getByteSpeed() - speed.getByteSpeed());
  }

  public Speed multiply(double num) {
    return Speed.toSpeed(this.getByteSpeed() * num);
  }

  public Speed divide(double num) {
    return Speed.toSpeed(this.getByteSpeed() / num);
  }

  @Override
  public String toString() {
    return df.format(this.getSpeed()) + this.getUnit() + "/S";
  }

  public static Speed parse(String speed) {
    Speed rsize = Speed.toSpeed(0);
    if (speed == null)
      speed = "";
    speed = speed.trim();
    if (!speed.isEmpty()) {
      Matcher m = pattern.matcher(speed.toUpperCase());
      if (m.matches() && m.groupCount() == 5) {
        String n = m.group(1);
        String u = m.group(5);
        Unit unit = Unit.parse(u);
        if (unit == null) {
          rsize = new Speed(Double.parseDouble(n));
        } else {
          rsize = new Speed(Double.parseDouble(n) * unit.getRate());
        }
      }
    }
    return rsize;
  }

  public static Speed toSpeed(double byteSpeed) {
    Unit unit = Unit.machUnit(byteSpeed);
    return new Speed(unit.machSize(byteSpeed), unit);
  }

  public static void main(String[] args) {
    System.out.println(Speed.parse("12345678 b/s"));

    System.out.println(Speed.parse("123e4 /s"));
  }

}
