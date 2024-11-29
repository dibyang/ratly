package net.xdob.ratly.util;


import java.util.Optional;
import java.util.function.Function;

/**
 * Finder
 *
 * @author yangzj
 * @version 1.0
 */
public class Finder {
  public static Function<Finder,Finder> nullToParent = f->f;

  public static final Finder NULL = c(null);


  /**
   * IndexFun
   *
   * @author yangzj
   * @version 1.0
   */
  @FunctionalInterface
  public interface IndexFun {
    int index(boolean last,String value, Integer fromIndex);

    static int head(boolean last, String value, Integer fromIndex){
      return 0;
    }
    static int tail(boolean last, String value, Integer fromIndex){
      return value.length();
    }
  }

  /**
   * BeginMark
   *
   * @author yangzj
   * @version 1.0
   */
  static class BeginMark implements IndexFun {
    private final String mark;
    private final int skip;

    BeginMark(String mark, int skip) {
      this.skip = Math.max(skip, 0);
      this.mark = mark;
    }

    @Override
    public int index(boolean last, String value, Integer fromIndex) {
      if (fromIndex == null) {
        fromIndex = 0;
      }
      if(last){
        int lastIndex = value.length();
        for(int i=0;i<=skip;i++){
          lastIndex = value.lastIndexOf(mark,lastIndex-1);
          if(lastIndex<fromIndex){
            break;
          }
        }
        if(lastIndex>=fromIndex){
          lastIndex = lastIndex + mark.length();
        }
        return lastIndex;
      }else {
        for (int i = 0; i <= skip; i++) {
          fromIndex = value.indexOf(mark, fromIndex);
          if (fromIndex < 0) {
            break;
          }
          fromIndex += mark.length();
        }
        return fromIndex;
      }
    }


    public static BeginMark c(String mark){
      return c(mark,0);
    }
    public static BeginMark c(String mark, int skip){
      return new BeginMark(mark,skip);
    }

  }

  /**
   * EndMark
   *
   * @author yangzj
   * @version 1.0
   */
  static class EndMark implements IndexFun {
    private final String mark;
    private final int skip;

    EndMark(String mark, int skip) {
      this.skip = Math.max(skip, 0);
      this.mark = mark;
    }

    @Override
    public int index(boolean last, String value, Integer fromIndex) {
      if (fromIndex == null) fromIndex = 0;
      if(last){
        int lastIndex = value.length();
        for (int i = 0; i <= skip; i++) {
          lastIndex = value.lastIndexOf(mark, lastIndex-1);
          if (lastIndex < fromIndex) {
            break;
          }
        }
        return lastIndex;
      }else {
        int index = 0;
        for (int i = 0; i <= skip; i++) {
          index = value.indexOf(mark, fromIndex);
          if (index < 0) {
            break;
          }
          fromIndex = index + mark.length();
        }
        return index;
      }
    }

    public static EndMark c(String mark){
      return c(mark,0);
    }

    public static EndMark c(String mark, int skip){
      return new EndMark(mark,skip);
    }
  }

  private final String value;
  private volatile boolean last;

  public Finder(String value,boolean last) {
    this.value = value;
    this.last = last;
  }

  public boolean isLast() {
    return last;
  }

  public Finder last(){
    last = true;
    return this;
  }

  public Finder before(){
    last = false;
    return this;
  }

  public boolean isNull(){
    return value==null;
  }

  public String getValue() {
    return value;
  }

  public Finder find(String beginMark, IndexFun end){
    return find(beginMark,end,null);
  }

  public Finder find(String beginMark, IndexFun end, Function<Finder,Finder> orElse){
    return find(BeginMark.c(beginMark),end, orElse);
  }

  public Finder find(IndexFun begin, String endMark){
    return find(begin, EndMark.c(endMark), null);
  }

  public Finder find(IndexFun begin, String endMark, Function<Finder,Finder> orElse){
    return find(begin, EndMark.c(endMark), orElse);
  }

  public Finder find(String beginMark, String endMark){
    return find(BeginMark.c(beginMark), EndMark.c(endMark), null);
  }

  public Finder find(String beginMark, String endMark, Function<Finder,Finder> orElse){
    return find(BeginMark.c(beginMark), EndMark.c(endMark), orElse);
  }

  public Finder find(IndexFun begin, IndexFun end){
    return find(begin,end,null);
  }

  public Finder find(IndexFun begin, IndexFun end, Function<Finder,Finder> orElse){
    if(value!=null) {
      int beginIndex = begin.index(last, value, 0);
      int endIndex = end.index(last, value, beginIndex);
      if (beginIndex >= 0 && endIndex >= beginIndex) {
        return c(value.substring(beginIndex, endIndex));
      }
      if(orElse!=null){
        return orElse.apply(this);
      }
    }
    return NULL;
  }

  public Finder head(String endMark, int skip){
    return head(EndMark.c(endMark,skip));
  }

  public Finder head(String endMark){
    return head(EndMark.c(endMark));
  }

  public Finder head(IndexFun end){
    return find(IndexFun::head,end);
  }

  public Finder tail(String beginMark, int skip){
    return tail(BeginMark.c(beginMark,skip));
  }

  public Finder tail(String beginMark){
    return tail(BeginMark.c(beginMark));
  }

  public Finder tail(IndexFun begin){
    return find(begin, IndexFun::tail);
  }

  public Finder head(String endMark, int skip, Function<Finder,Finder> orElse){
    return head(EndMark.c(endMark,skip),orElse);
  }

  public Finder head(String endMark, Function<Finder,Finder> orElse){
    return head(EndMark.c(endMark),orElse);
  }

  public Finder head(IndexFun end, Function<Finder,Finder> orElse){
    return find(IndexFun::head,end,orElse);
  }

  public Finder tail(String beginMark, int skip, Function<Finder,Finder> orElse){
    return tail(BeginMark.c(beginMark,skip),orElse);
  }

  public Finder tail(String beginMark, Function<Finder,Finder> orElse){
    return tail(BeginMark.c(beginMark),orElse);
  }

  public Finder tail(IndexFun begin, Function<Finder,Finder> orElse){
    return find(begin, IndexFun::tail,orElse);
  }

  public <T> Optional<T> getNullableValue(Class<T> clazz)
  {
    return Optional.ofNullable(getValue(clazz));
  }

  public Optional<String> getNullableValue()
  {
    return Optional.ofNullable(getValue());
  }

  public <T> T getValue(Class<T> clazz)
  {
    return Types.cast(value, clazz);
  }

  public static  Finder c(String value){
    return c(value,false);
  }

  public static  Finder c(String value, boolean last){
    return new Finder(value,last);
  }



  public static void main(String[] args) {
    String s="user2:$6$/5C7Cg06$sMjvN8/dPSCgdng32oeJ8TqsQmkgxGeqA7qHX3Eurw0EN6lam7GSVcP8M9TM0/t80WiV9jDDRHltpuEY.CfMM/:18522:0:99999:7:::";
    Finder finder = Finder.c(s);
    String username = finder.head(":").getValue();
    System.out.println("username = " + username);
    String pwd = finder.tail(":").head(":").getValue();
    System.out.println("pwd = " + pwd);
    String pwd2 = finder.head(":",1).tail(":").getValue();
    System.out.println("pwd2 = " + pwd2);
    String s2="tuser:x:10001:10004::/home/tuser:/bin/bash";
    Finder finder2 = Finder.c(s2);
    String uid = finder2.tail(":", 1).head(":").getValue();
    String gid = finder2.tail(":", 2).head(":").getValue();
    System.out.println("uid = " + uid);
    System.out.println("gid = " + gid);
    finder2.last();
    String s21 = finder2.head(":").getValue();
    String s22 = finder2.head(":",2).getValue();
    String s23 = finder2.tail(":").getValue();
    String s24 = finder2.tail(":",2).getValue();
    System.out.println("s21 = " + s21);
    System.out.println("s22 = " + s22);
    System.out.println("s23 = " + s23);
    System.out.println("s24 = " + s24);
    Finder finder3 = Finder.c("#IPADDR=");
    System.out.println(finder3.head("=").getValue());
    String route1 = "default via 13.13.13.253 dev br0 proto static";
    String route2 = "default via 13.13.13.253 dev br0 ";
    String dev1 = Finder.c(route1).tail("dev ").head(" ",f->f).getValue().trim();
    String dev2 = Finder.c(route2).tail("dev ").head(" ",f->f).getValue().trim();
    System.out.println("dev1 =" + dev1+ " dev2 =" + dev2);
  }

}
