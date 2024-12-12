package net.xdob.ratly.fasts.serialization.util;

public interface FSTInt2ObjectMapFactory {
    <V> FSTInt2ObjectMap<V> createMap(int size);
}