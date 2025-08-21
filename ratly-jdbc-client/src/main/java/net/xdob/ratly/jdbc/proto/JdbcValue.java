package net.xdob.ratly.jdbc.proto;

import net.xdob.ratly.jdbc.sql.SerialRowId;
import net.xdob.ratly.proto.jdbc.JdbcValueProto;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import java.io.*;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;

public class JdbcValue {
	private Object value;
	private String type;

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	/**
	 * 将 JdbcParam 消息转换为对应的 Java 对象
	 *
	 * @param param 输入的 JdbcParam 消息
	 * @return 转换后的 Java 对象
	 * @throws SQLException 当遇到不支持的类型或转换错误时抛出
	 */
	public static Object toJavaObject(JdbcValueProto  param) throws SQLException {
		switch (param.getValueCase()) {
			// 基本类型
			case BOOLEAN_VALUE:
				return param.getBooleanValue();
			case INT_VALUE:
				return param.getIntValue();
			case LONG_VALUE:
				return param.getLongValue();
			case FLOAT_VALUE:
				return param.getFloatValue();
			case DOUBLE_VALUE:
				return param.getDoubleValue();
			case STRING_VALUE:
				return param.getStringValue();
			case BYTES_VALUE:
				return param.getBytesValue().toByteArray();

			// 日期时间类型
			case DATE_VALUE: {
				JdbcValueProto.Date d = param.getDateValue();
				return java.sql.Date.valueOf(
						LocalDate.of(d.getYear(), d.getMonth(), d.getDay()));
			}
			case TIME_VALUE: {
				JdbcValueProto.Time t = param.getTimeValue();
				return java.sql.Time.valueOf(
						LocalTime.of(t.getHour(), t.getMinute(), t.getSecond(), t.getNanos()));
			}
			case TIMESTAMP_VALUE: {
				com.google.protobuf.Timestamp ts = param.getTimestampValue();
				long millis = ts.getSeconds() * 1000 + ts.getNanos() / 1_000_000;
				return new java.sql.Timestamp(millis);
			}

			// 精确数值类型
			case DECIMAL_VALUE:
				return new BigDecimal(param.getDecimalValue());

			// 可空包装器类型
			case NULLABLE_BOOL:
				return param.getNullableBool().getValue();
			case NULLABLE_INT:
				return param.getNullableInt().getValue();
			case NULLABLE_LONG:
				return param.getNullableLong().getValue();
			case NULLABLE_FLOAT:
				return param.getNullableFloat().getValue();
			case NULLABLE_DOUBLE:
				return param.getNullableDouble().getValue();
			case NULLABLE_STRING:
				return param.getNullableString().getValue();
			case NULLABLE_BYTES:
				return param.getNullableBytes().getValue().toByteArray();

			// 特殊 SQL 类型
			case NULL_VALUE:
				return null;

			case REF_VALUE:
				return param.getRefValue().getRefCursorName();

			case BLOB_VALUE:
				return new SerialBlob(param.getBlobValue().getData().toByteArray());

			case CLOB_VALUE:
				return new SerialClob(param.getClobValue().getData().toCharArray());

			case NCLOB_VALUE:
				return new SerialClob(param.getNclobValue().getData().toCharArray());

			case SQLXML_VALUE:
				return param.getSqlxmlValue().getXml();

			case JSON_VALUE:
				return param.getJsonValue().getJson();

			case URL_VALUE:
				return param.getUrlValue().getUrl();

			// UUID 类型
			case UUID_VALUE: {
				JdbcValueProto.Uuid uuid = param.getUuidValue();

				if (!uuid.getBinaryValue().isEmpty()) {
					ByteBuffer bb = ByteBuffer.wrap(uuid.getBinaryValue().toByteArray());
					long high = bb.getLong();
					long low = bb.getLong();
					return new UUID(high, low);
				} else {
					return UUID.fromString(uuid.getStringValue());
				}
			}

			// 间隔类型
			case INTERVAL_VALUE: {
				JdbcValueProto.Interval interval = param.getIntervalValue();
				if (!interval.getIsoString().isEmpty()) {
					return Duration.parse(interval.getIsoString());
				} else {
					// 根据字段值构造 Duration
					long totalSeconds = interval.getSeconds() +
							interval.getMinutes() * 60 +
							interval.getHours() * 3600 +
							interval.getDays() * 86400;
					long nanos = interval.getNanos();
					Duration duration = Duration.ofSeconds(totalSeconds, nanos);
					return interval.getIsNegative() ? duration.negated() : duration;
				}
			}

			// 数组类型
			case ARRAY_VALUE: {
				JdbcValueProto.Array arrayValue = param.getArrayValue();

				String elementTypeName = arrayValue.getElementTypeName();
				try {
					Class<?> componentType  = Class.forName(elementTypeName);
					Object array = Array.newInstance(componentType, arrayValue.getElementsCount());
					List<JdbcValueProto> elementsList = arrayValue.getElementsList();
					for (int i = 0; i < elementsList.size(); i++) {
						Array.set(array, i, toJavaObject(elementsList.get(i)));
					}
					return array;
				} catch (ClassNotFoundException e) {
					throw new SQLException("ClassNotFound:" + elementTypeName, e);
				}

			}

			// 结构类型
			case STRUCT_VALUE: {
				JdbcValueProto.Struct struct = param.getStructValue();
				Map<String, Object> attributes = new HashMap<>();
				for (JdbcValueProto.Struct.Attribute attr : struct.getAttributesList()) {
					attributes.put(attr.getName(), toJavaObject(attr.getValue()));
				}
				return attributes;
			}

			case ROW_ID_VALUE:
				return new SerialRowId(param.getRowIdValue().getRowId().toString());

			// 自定义对象
			case CUSTOM_VALUE: {
				JdbcValueProto.CustomObject custom = param.getCustomValue();
				try {
					ObjectInputStream ois = new ObjectInputStream(
							new ByteArrayInputStream(custom.getSerializedData().toByteArray()));
					return ois.readObject();
				} catch (IOException | ClassNotFoundException e) {
					throw new SQLException("Failed to deserialize custom object", e);
				}
			}

			// 未处理类型
			default:
				throw new SQLException("Unsupported parameter type: " + param.getValueCase());
		}
	}

	public void setValue(JdbcValueProto  proto) throws SQLException {
		value = toJavaObject(proto);
	}

	public static JdbcValueProto toValueProto(Object value) {
		JdbcValueProto.Builder builder = JdbcValueProto.newBuilder();

		if (value == null) {
			builder.setNullValue(JdbcValueProto.NullValue.NULL_VALUE);
		} else if (value instanceof Boolean) {
			builder.setBooleanValue((Boolean) value);
		} else if (value instanceof Integer) {
			builder.setIntValue((Integer) value);
		} else if (value instanceof Long) {
			builder.setLongValue((Long) value);
		} else if (value instanceof Float) {
			builder.setFloatValue((Float) value);
		} else if (value instanceof Double) {
			builder.setDoubleValue((Double) value);
		} else if (value instanceof String) {
			builder.setStringValue((String) value);
		} else if (value instanceof byte[]) {
			builder.setBytesValue(com.google.protobuf.ByteString.copyFrom((byte[]) value));
		} else if (value instanceof java.sql.Date) {
			java.sql.Date date = (java.sql.Date) value;
			LocalDate localDate = date.toLocalDate();
			JdbcValueProto.Date.Builder dateBuilder = JdbcValueProto.Date.newBuilder();
			dateBuilder.setYear(localDate.getYear());
			dateBuilder.setMonth(localDate.getMonthValue());
			dateBuilder.setDay(localDate.getDayOfMonth());
			builder.setDateValue(dateBuilder.build());
		} else if (value instanceof java.sql.Time) {
			java.sql.Time time = (java.sql.Time) value;
			LocalTime localTime = time.toLocalTime();
			JdbcValueProto.Time.Builder timeBuilder = JdbcValueProto.Time.newBuilder();
			timeBuilder.setHour(localTime.getHour());
			timeBuilder.setMinute(localTime.getMinute());
			timeBuilder.setSecond(localTime.getSecond());
			timeBuilder.setNanos(localTime.getNano());
			builder.setTimeValue(timeBuilder.build());
		} else if (value instanceof java.sql.Timestamp) {
			java.sql.Timestamp timestamp = (java.sql.Timestamp) value;
			com.google.protobuf.Timestamp.Builder timestampBuilder = com.google.protobuf.Timestamp.newBuilder();
			timestampBuilder.setSeconds(timestamp.getTime() / 1000);
			timestampBuilder.setNanos(timestamp.getNanos());
			builder.setTimestampValue(timestampBuilder.build());
		} else if (value instanceof java.util.Date) {
			java.util.Date date = (java.util.Date) value;
			com.google.protobuf.Timestamp.Builder timestampBuilder = com.google.protobuf.Timestamp.newBuilder();
			timestampBuilder.setSeconds(date.getTime() / 1000);
			timestampBuilder.setNanos(0);
			builder.setTimestampValue(timestampBuilder.build());
		} else if (value instanceof BigDecimal) {
			builder.setDecimalValue(((BigDecimal) value).toString());
		} else if (value instanceof UUID) {
			UUID uuid = (UUID) value;
			JdbcValueProto.Uuid.Builder uuidBuilder = JdbcValueProto.Uuid.newBuilder();
			uuidBuilder.setStringValue(uuid.toString());
			ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
			bb.putLong(uuid.getMostSignificantBits());
			bb.putLong(uuid.getLeastSignificantBits());
			uuidBuilder.setBinaryValue(com.google.protobuf.ByteString.copyFrom(bb.array()));
			builder.setUuidValue(uuidBuilder.build());
		} else if (value.getClass().isArray()) {
			JdbcValueProto.Array.Builder arrayBuilder = JdbcValueProto.Array.newBuilder();
			int length = Array.getLength(value);
			for (int i = 0; i < length; i++) {
				JdbcValueProto valueProto = toValueProto(Array.get(value, i));
				arrayBuilder.addElements(valueProto);
			}
			arrayBuilder.setElementTypeName(value.getClass().getComponentType().getName());
			builder.setArrayValue(arrayBuilder.build());

		} else if (value instanceof Map) {
			Map map = (Map) value;
			JdbcValueProto.Struct.Builder stBuilder = JdbcValueProto.Struct.newBuilder();
			for (Object entryObj : map.entrySet()) {
				Map.Entry entry = (Map.Entry) entryObj;
				if (entry.getKey() != null) {
					Object val = entry.getValue();
					stBuilder.addAttributes(
							JdbcValueProto.Struct.Attribute.newBuilder()
									.setName(entry.getKey().toString())
									.setValue(toValueProto(val))
									.build()
					);
				}
			}
			builder.setStructValue(stBuilder.build());
		} else {
			try {
				ByteArrayOutputStream out = new ByteArrayOutputStream();
				ObjectOutputStream ois = new ObjectOutputStream(out);
				ois.writeObject(value);
				builder.setCustomValue(JdbcValueProto.CustomObject.newBuilder()
						.setClassName(value.getClass().getName())
						.setSerializedData(com.google.protobuf.ByteString.copyFrom(out.toByteArray()))
						.build());
			} catch (IOException e) {
				throw new RuntimeException("Failed to serialize custom object", e);
			}
		}

		return builder.build();
	}

	public JdbcValueProto toProto() {
		return toValueProto(value);
	}

	public static void main(String[] args) throws SQLException {
		List<Object> values = new ArrayList<>();
//		values.add(123);
//		values.add(new BigDecimal("123.456"));
//		values.add(new java.util.Date());
//		values.add(new java.sql.Date(System.currentTimeMillis()));
//		values.add(new java.sql.Time(System.currentTimeMillis()));
//		values.add(new java.sql.Timestamp(System.currentTimeMillis()));
//		values.add(new UUID(123, 456));
//		values.add(456L);
//		values.add(8.92);
//		values.add("test is ok");
		values.add(new Object[]{"t1","t2",new String[]{"a1","b2"}});
		for (Object value : values) {
			JdbcValueProto valueProto = JdbcValue.toValueProto(value);
			Object val = JdbcValue.toJavaObject(valueProto);
			System.out.println("value = " + value);
			System.out.println("val.getClass() = " + val.getClass());
			System.out.println("val = " + val);
		}

	}
}
