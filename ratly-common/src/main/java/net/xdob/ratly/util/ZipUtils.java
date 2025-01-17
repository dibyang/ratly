package net.xdob.ratly.util;

import java.io.*;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.zip.*;

public class ZipUtils {

  /**
   * 压缩
   *
   * @param data 待压缩数据
   * @return byte[] 压缩后的数据
   */
  public static byte[] compress(byte[] data) {
    
      byte[] output = new byte[0];

      Deflater compresser = new Deflater();

      compresser.reset();
      compresser.setInput(data);
      compresser.finish();
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      try {
          byte[] buf = new byte[1024];
          while (!compresser.finished()) {
              int i = compresser.deflate(buf);
              bos.write(buf, 0, i);
          }
          output = bos.toByteArray();
      } catch (Exception e) {
          output = data;
          e.printStackTrace();
      } finally {
          try {
              bos.close();
          } catch (IOException e) {
              e.printStackTrace();
          }
      }
      compresser.end();
      return output;
  }

  /**
   * 解压缩
   *
   * @param data 待压缩的数据
   * @return byte[] 解压缩后的数据
   */
  public static byte[] decompress(byte[] data) {
      byte[] output = new byte[0];

      Inflater decompresser = new Inflater();
      decompresser.reset();
      decompresser.setInput(data);

      ByteArrayOutputStream o = new ByteArrayOutputStream();
      try {
          byte[] buf = new byte[1024];
          while (!decompresser.finished()) {
              int i = decompresser.inflate(buf);
              o.write(buf, 0, i);
          }
          output = o.toByteArray();
      } catch (Exception e) {
          output = data;
          e.printStackTrace();
      } finally {
          try {
              o.close();
          } catch (IOException e) {
              e.printStackTrace();
          }
      }

      decompresser.end();
      return output;
  }

  public static void compressFiles(String zipFilePath, String... filePaths) throws IOException {
    compressFiles(new File(zipFilePath), Arrays.stream(filePaths)
        .map(File::new).toArray(File[]::new));
  }

  public static void compressFiles(File zipFile, File... files) throws IOException {
    try(FileOutputStream fos = new FileOutputStream(zipFile);
        ZipOutputStream zos = new ZipOutputStream(fos)) {
      for (File file : files) {
        if(file.exists()) {
          try (FileInputStream fis = new FileInputStream(file)) {
            ZipEntry zipEntry = new ZipEntry(file.getName());
            zos.putNextEntry(zipEntry);
            byte[] buffer = new byte[1024];
            int length;
            while ((length = fis.read(buffer)) > 0) {
              zos.write(buffer, 0, length);
            }
          }
        }
      }
    }
  }

  public static void decompressFiles(File zipFile, File destDir) throws IOException {
    try(FileInputStream fis = new FileInputStream(zipFile);
        ZipInputStream zis = new ZipInputStream(fis);) {
      ZipEntry ze = zis.getNextEntry();
      byte[] buffer = new byte[1024];
      int length;
      while (ze != null) {
        String fileName = ze.getName();
        if(!destDir.exists()){
          destDir.mkdirs();
        }
        try(FileOutputStream fos = new FileOutputStream(destDir.toPath().resolve(fileName).toFile())) {
          while ((length = zis.read(buffer)) > 0) {
            fos.write(buffer, 0, length);
          }
        }
        ze = zis.getNextEntry();
      }
    }
  }
  public static void decompressFiles(String zipFilePath, String destDir) throws IOException {
    decompressFiles(new File(zipFilePath), new File(destDir));
  }



}
