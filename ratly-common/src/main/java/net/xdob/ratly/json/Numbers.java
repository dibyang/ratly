package net.xdob.ratly.json;

public class Numbers {

  public static double p(double number, int digits) {
    if (digits < 0)
      digits = 0;
    double p = Math.pow(10, digits);
    double round = Math.round(number * p);
    return round / p;
  }

  public static double p2(double number) {
    return p(number, 2);
  }

  public static double p3(double number) {
    return p(number, 3);
  }

  public static void main(String[] args) {
    for (int i = 0; i < 10; i++)
      System.out.println(p(1.022380212, i));
  }
}
