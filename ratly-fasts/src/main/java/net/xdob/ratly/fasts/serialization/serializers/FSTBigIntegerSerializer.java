package net.xdob.ratly.fasts.serialization.serializers;

import net.xdob.ratly.fasts.serialization.FSTBasicObjectSerializer;
import net.xdob.ratly.fasts.serialization.FSTClazzInfo;
import net.xdob.ratly.fasts.serialization.FSTObjectInput;
import net.xdob.ratly.fasts.serialization.FSTObjectOutput;

import java.io.IOException;
import java.math.BigInteger;

/**
 * author: nicoruti
 * submitted via #53
 *
 */
public class FSTBigIntegerSerializer  extends FSTBasicObjectSerializer {

    @Override
    public void writeObject(FSTObjectOutput out, Object toWrite, FSTClazzInfo clzInfo, FSTClazzInfo.FSTFieldInfo referencedBy,
                            int streamPosition) throws IOException {
        byte[] value = ((BigInteger) toWrite).toByteArray();
        out.writeInt(value.length);
        out.write(value);
    }

    @Override
    public Object instantiate(Class objectClass, FSTObjectInput in, FSTClazzInfo serializationInfo, FSTClazzInfo.FSTFieldInfo referencee,
                              int streamPosition) throws Exception {
        int len = in.readInt();
        byte[] buf = new byte[len];
        in.read(buf);
        BigInteger bigInteger = new BigInteger(buf);
        in.registerObject(bigInteger,streamPosition,serializationInfo,referencee);
        return bigInteger;
    }
}
