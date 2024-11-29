package net.xdob.ratly.json;

/**
 * 容量大小单位
 *
 * @author 杨志坚 Email: dib.yang@gmail.com
 *
 */
public enum Unit {
  bytes(1), KB(bytes.getRate() * 1024), MB(KB.getRate() * 1024), GB(MB.getRate() * 1024), TB(GB
      .getRate() * 1024), PB(TB.getRate() * 1024), EB(PB.getRate() * 1024), ZB(EB.getRate() * 1024);

  private final double rate;

  public double getRate() {
    return rate;
  }

  public double machSize(double byteSize) {
    double num = byteSize / rate;
    return p3(num);
  }

  private double p3(double n) {
    return Numbers.p3(n);
  }

  Unit(double rate) {
    this.rate = rate;
  }

  public static Unit parse(String name) {
    Unit unit = null;
    if (name != null) {
      name = name.trim();
      if (!name.isEmpty()) {
        if (name.equals("B") || name.equalsIgnoreCase("bytes")) {
          unit = bytes;
        } else {
          name = name.toUpperCase();
          if (name.length() == 1)
            name = name + "B";
          unit = Unit.valueOf(name);
        }

      }
    }
    return unit;
  }

  public static Unit machUnit(double byteSize) {
    if (byteSize <= 0)
      return Unit.bytes;
    if (byteSize < Unit.MB.getRate()) {
      return Unit.KB;
    } else if (byteSize < Unit.GB.getRate()) {
      return Unit.MB;
    } else if (byteSize < Unit.TB.getRate()) {
      return Unit.GB;
    } else if (byteSize < Unit.PB.getRate()) {
      return Unit.TB;
    } else if (byteSize < Unit.EB.getRate()) {
      return Unit.PB;
    } else if (byteSize < Unit.ZB.getRate()) {
      return Unit.EB;
    } else {
      return Unit.ZB;
    }
  }

}
