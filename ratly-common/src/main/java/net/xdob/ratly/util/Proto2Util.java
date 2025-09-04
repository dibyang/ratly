package net.xdob.ratly.util;

import com.google.protobuf.ByteString;
import net.xdob.ratly.proto.base.Throwable2Proto;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.sql.SQLException;

public interface Proto2Util {

	static ByteString writeObject2ByteString(Object obj) {
		final ByteString.Output byteOut = ByteString.newOutput();
		try(ObjectOutputStream objOut = new ObjectOutputStream(byteOut)) {
			objOut.writeObject(obj);
		} catch (IOException e) {
			throw new IllegalStateException(
					"Unexpected IOException when writing an object to a ByteString.", e);
		}
		return byteOut.toByteString();
	}

	static Object toObject(ByteString bytes) {
		return IOUtils.readObject(bytes.newInput(), Object.class);
	}

	static Throwable2Proto toThrowable2Proto(Throwable t) {
		final Throwable2Proto.Builder builder = Throwable2Proto.newBuilder()
				.setClassName(t.getClass().getName())
				.setErrorMessage(t.getMessage())
				.setThrowable(writeObject2ByteString(t));
		return builder.build();
	}

	static SQLException toSQLException(Throwable2Proto proto) {
		return toThrowable(proto, SQLException.class);
	}

	static <T extends Throwable> T toThrowable(Throwable2Proto proto, Class<T> clazz) {

		final Throwable throwable ;
		try {
			throwable = (Throwable)toObject(proto.getThrowable());
		} catch(Exception e) {
			throw new IllegalStateException("Failed to create a new object from " + Throwable.class + ", proto=" + proto, e);
		}
		Preconditions.assertTrue(proto.getClassName().equals(throwable.getClass().getName()),
				() -> "Unexpected class " + throwable.getClass() + ", expecting " + proto.getClassName() + ", proto=" + proto);

		return JavaUtils.cast(throwable, clazz);
	}
}
