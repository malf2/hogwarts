import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.*;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public class AvroConverter {

    public static GenericRecord toGenericRecord(Map<String, Object> input, Schema schema) {
        GenericRecord record = new GenericData.Record(schema);

        for (Schema.Field field : schema.getFields()) {
            Object rawValue = input.get(field.name());
            if (rawValue == null) {
                record.put(field.name(), null);
                continue;
            }

            Schema fieldSchema = getNonNullable(field.schema());
            LogicalTypes.LogicalType logicalType = fieldSchema.getLogicalType();

            Object converted;
            if (logicalType != null) {
                converted = convertLogicalType(rawValue, fieldSchema, logicalType);
            } else {
                converted = convertPrimitiveType(rawValue, fieldSchema);
            }

            record.put(field.name(), converted);
        }

        return record;
    }

    // --- Logical types handling ---
    private static Object convertLogicalType(Object value, Schema schema, LogicalTypes.LogicalType logicalType) {
        switch (logicalType.getName()) {
            case "date":
                return toAvroDate(value);
            case "time-millis":
                return toAvroTimeMillis(value);
            case "time-micros":
                return toAvroTimeMicros(value);
            case "timestamp-millis":
                return toAvroTimestampMillis(value);
            case "timestamp-micros":
                return toAvroTimestampMicros(value);
            case "local-timestamp-millis":
                return toAvroLocalTimestampMillis(value);
            case "local-timestamp-micros":
                return toAvroLocalTimestampMicros(value);
            case "decimal":
                return toAvroDecimal(value, schema);
            default:
                throw new IllegalArgumentException("Unsupported logical type: " + logicalType.getName());
        }
    }

    // --- Primitive types handling ---
    private static Object convertPrimitiveType(Object value, Schema schema) {
        switch (schema.getType()) {
            case STRING:
                return value.toString();
            case INT:
                return toInt(value);
            case LONG:
                return toLong(value);
            case FLOAT:
                return toFloat(value);
            case DOUBLE:
                return toDouble(value);
            case BOOLEAN:
                return toBoolean(value);
            case BYTES:
                return toBytes(value);
            case ARRAY:
                return toArray((List<?>) value, schema.getElementType());
            case RECORD:
                if (value instanceof Map) {
                    return convert((Map<String, Object>) value, schema);
                }
                throw new IllegalArgumentException("Expected Map for RECORD, got: " + value.getClass());
            case UNION:
                return convertPrimitiveType(value, getNonNullable(schema));
            default:
                throw new IllegalArgumentException("Unsupported Avro type: " + schema.getType());
        }
    }

    // --- Logical converters ---
    private static int toAvroDate(Object value) {
        if (value instanceof LocalDate) return (int) ((LocalDate) value).toEpochDay();
        if (value instanceof String) return (int) LocalDate.parse((String) value).toEpochDay();
        throw new IllegalArgumentException("Cannot convert to Avro date: " + value);
    }

    private static int toAvroTimeMillis(Object value) {
        if (value instanceof LocalTime) return ((LocalTime) value).toNanoOfDay() / 1_000_000;
        throw new IllegalArgumentException("Cannot convert to Avro time-millis: " + value);
    }

    private static long toAvroTimeMicros(Object value) {
        if (value instanceof LocalTime) return ((LocalTime) value).toNanoOfDay() / 1_000;
        throw new IllegalArgumentException("Cannot convert to Avro time-micros: " + value);
    }

    private static long toAvroTimestampMillis(Object value) {
        if (value instanceof OffsetDateTime) return ((OffsetDateTime) value).toInstant().toEpochMilli();
        if (value instanceof Instant) return ((Instant) value).toEpochMilli();
        throw new IllegalArgumentException("Cannot convert to Avro timestamp-millis: " + value);
    }

    private static long toAvroTimestampMicros(Object value) {
        if (value instanceof OffsetDateTime) {
            Instant i = ((OffsetDateTime) value).toInstant();
            return i.getEpochSecond() * 1_000_000 + i.getNano() / 1_000;
        }
        if (value instanceof Instant) {
            Instant i = (Instant) value;
            return i.getEpochSecond() * 1_000_000 + i.getNano() / 1_000;
        }
        throw new IllegalArgumentException("Cannot convert to Avro timestamp-micros: " + value);
    }

    private static long toAvroLocalTimestampMillis(Object value) {
        if (value instanceof LocalDateTime) {
            LocalDateTime ldt = (LocalDateTime) value;
            return ldt.toEpochSecond(ZoneOffset.UTC) * 1000 + ldt.getNano() / 1_000_000;
        }
        throw new IllegalArgumentException("Cannot convert to Avro local-timestamp-millis: " + value);
    }

    private static long toAvroLocalTimestampMicros(Object value) {
        if (value instanceof LocalDateTime) {
            LocalDateTime ldt = (LocalDateTime) value;
            return ldt.toEpochSecond(ZoneOffset.UTC) * 1_000_000 + ldt.getNano() / 1_000;
        }
        throw new IllegalArgumentException("Cannot convert to Avro local-timestamp-micros: " + value);
    }

    private static ByteBuffer toAvroDecimal(Object value, Schema schema) {
        int scale = schema.getObjectProp("scale") != null ? (Integer) schema.getObjectProp("scale") : 0;
        BigDecimal decimal;
        if (value instanceof BigDecimal) {
            decimal = (BigDecimal) value;
        } else if (value instanceof String) {
            decimal = new BigDecimal((String) value);
        } else {
            throw new IllegalArgumentException("Cannot convert to Avro decimal: " + value);
        }
        decimal = decimal.setScale(scale);
        return ByteBuffer.wrap(decimal.unscaledValue().toByteArray());
    }

    // --- Primitive converters ---
    private static int toInt(Object value) {
        if (value instanceof Number) return ((Number) value).intValue();
        if (value instanceof String) return Integer.parseInt((String) value);
        throw new IllegalArgumentException("Cannot convert to int: " + value);
    }

    private static long toLong(Object value) {
        if (value instanceof Number) return ((Number) value).longValue();
        if (value instanceof String) return Long.parseLong((String) value);
        throw new IllegalArgumentException("Cannot convert to long: " + value);
    }

    private static float toFloat(Object value) {
        if (value instanceof Number) return ((Number) value).floatValue();
        if (value instanceof String) return Float.parseFloat((String) value);
        throw new IllegalArgumentException("Cannot convert to float: " + value);
    }

    private static double toDouble(Object value) {
        if (value instanceof Number) return ((Number) value).doubleValue();
        if (value instanceof String) return Double.parseDouble((String) value);
        throw new IllegalArgumentException("Cannot convert to double: " + value);
    }

    private static boolean toBoolean(Object value) {
        if (value instanceof Boolean) return (Boolean) value;
        if (value instanceof String) return Boolean.parseBoolean((String) value);
        throw new IllegalArgumentException("Cannot convert to boolean: " + value);
    }

    private static ByteBuffer toBytes(Object value) {
        if (value instanceof byte[]) return ByteBuffer.wrap((byte[]) value);
        if (value instanceof String) return ByteBuffer.wrap(Base64.getDecoder().decode((String) value));
        throw new IllegalArgumentException("Cannot convert to bytes: " + value);
    }

    private static Object toArray(List<?> list, Schema elementSchema) {
        GenericData.Array<Object> array = new GenericData.Array<>(list.size(), elementSchema);
        for (Object elem : list) {
            array.add(convertPrimitiveType(elem, elementSchema));
        }
        return array;
    }

    // --- Utility ---
    private static Schema getNonNullable(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            for (Schema s : schema.getTypes()) {
                if (s.getType() != Schema.Type.NULL) return s;
            }
        }
        return schema;
    }
}