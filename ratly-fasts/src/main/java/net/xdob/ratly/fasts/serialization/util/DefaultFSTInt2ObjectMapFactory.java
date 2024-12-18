package net.xdob.ratly.fasts.serialization.util;

public class DefaultFSTInt2ObjectMapFactory implements FSTInt2ObjectMapFactory{
    @Override
    public <V> FSTInt2ObjectMap<V> createMap(int size) {
        return new DefaultFSTInt2ObjectMap<>(size);
    }
}