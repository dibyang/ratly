package net.xdob.ratly.fasts.serialization.serializers;

import net.xdob.ratly.fasts.serialization.FSTBasicObjectSerializer;
import net.xdob.ratly.fasts.serialization.FSTClazzInfo;
import net.xdob.ratly.fasts.serialization.FSTObjectInput;
import net.xdob.ratly.fasts.serialization.FSTObjectOutput;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;

/**
 * Created by ruedi on 24/05/15.
 */
public class FSTJSonSerializers {

    public static class BigDecSerializer extends FSTBasicObjectSerializer {
        @Override
        public void writeObject(FSTObjectOutput out, Object toWrite, FSTClazzInfo clzInfo, FSTClazzInfo.FSTFieldInfo referencedBy, int streamPosition) throws IOException {
            out.writeStringUTF(toWrite.toString());
        }

        @Override
        public boolean alwaysCopy() {
            return true;
        }

        @Override
        public Object instantiate(Class objectClass, FSTObjectInput in, FSTClazzInfo serializationInfo, FSTClazzInfo.FSTFieldInfo referencee, int streamPosition) throws Exception {
            return new BigDecimal(in.readStringUTF());
        }
    }

    public static class TimestampSerializer extends FSTBasicObjectSerializer {
        @Override
        public void writeObject(FSTObjectOutput out, Object toWrite, FSTClazzInfo clzInfo, FSTClazzInfo.FSTFieldInfo referencedBy, int streamPosition) throws IOException {
            out.writeLong(((Timestamp) toWrite).getTime());
        }

        @Override
        public boolean alwaysCopy() {
            return true;
        }

        @Override
        public Object instantiate(Class objectClass, FSTObjectInput in, FSTClazzInfo serializationInfo, FSTClazzInfo.FSTFieldInfo referencee, int streamPosition) throws Exception {
            return new Timestamp(in.readLong());
        }
    }
}
