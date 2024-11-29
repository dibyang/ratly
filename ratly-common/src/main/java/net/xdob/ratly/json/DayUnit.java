package net.xdob.ratly.json;
/**
 * 分钟格式化单位
 * @author zhengxq
 *
 */
public enum DayUnit {
  minute(1), hour(minute.getRate() * 60), day(hour.getRate() * 24), month(day.getRate() * 30), year(
      day.getRate() * 365);

  private final long rate;

  public long getRate() {
    return rate;
  }

  DayUnit(long rate) {
    this.rate = rate;
  }

  public static DayUnit machUnit(long minute) {
    if (minute < 0)
      throw new IllegalArgumentException("byteSize < 0");
    if ((minute >= DayUnit.year.getRate()) && (minute % DayUnit.year.getRate() == 0)) {
      return DayUnit.year;
    } else if ((minute >= DayUnit.month.getRate()) && (minute % DayUnit.month.getRate() == 0)) {
      return DayUnit.month;
    } else if ((minute >= DayUnit.day.getRate()) && (minute % DayUnit.day.getRate() == 0)) {
      return DayUnit.day;
    } else if ((minute >= DayUnit.hour.getRate()) && (minute % DayUnit.hour.getRate() == 0)) {
      return DayUnit.hour;
    } else {
      return DayUnit.minute;
    }
  }

  public static DayUnit machUnit(String name) {
    DayUnit unit = null;
    if (name != null) {
      name = name.trim();
      if (!name.isEmpty()) {
        if (name.contains("year")) {
          unit = DayUnit.year;
        } else if (name.contains("month")) {
          return DayUnit.month;
        } else if (name.contains("day")) {
          return DayUnit.day;
        } else if (name.contains("hour")) {
          return DayUnit.hour;
        } else {
          return DayUnit.minute;
        }
      }
    }
    return unit;
  }
}