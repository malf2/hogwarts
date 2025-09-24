import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.Conversions;

import java.math.BigDecimal;
import java.time.*;
import java.util.Date;
import java.util.Map;

public class AvroConverter {

    public static GenericRecord toGenericRecord(Schema schema, Map<String, Object> values) {
        GenericRecord record = new GenericData.Record(schema);

        for (Schema.Field field : schema.getFields()) {
            Object value = values.get(field.name());
            Schema fieldSchema = unwrapUnion(field.schema());

            if (value == null) {
                record.put(field.name(), null);
                continue;
            }

            LogicalTypes.LogicalType logicalType = fieldSchema.getLogicalType();
            if (logicalType != null) {
                String name = logicalType.getName();
                switch (name) {
                    case "date": {
                        if (value instanceof LocalDate) {
                            record.put(field.name(), (int) ((LocalDate) value).toEpochDay());
                        } else if (value instanceof Date) {
                            LocalDate date = ((Date) value).toInstant().atZone(ZoneOffset.UTC).toLocalDate();
                            record.put(field.name(), (int) date.toEpochDay());
                        }
                        break;
                    }
                    case "time-millis": {
                        if (value instanceof LocalTime) {
                            long millis = ((LocalTime) value).toNanoOfDay() / 1_000_000;
                            record.put(field.name(), (int) millis);
                        }
                        break;
                    }
                    case "time-micros": {
                        if (value instanceof LocalTime) {
                            long micros = ((LocalTime) value).toNanoOfDay() / 1_000;
                            record.put(field.name(), micros);
                        }
                        break;
                    }
                    case "timestamp-millis": {
                        if (value instanceof Instant) {
                            record.put(field.name(), ((Instant) value).toEpochMilli());
                        } else if (value instanceof OffsetDateTime) {
                            record.put(field.name(), ((OffsetDateTime) value).toInstant().toEpochMilli());
                        }
                        break;
                    }
                    case "timestamp-micros": {
                        if (value instanceof Instant) {
                            record.put(field.name(), ((Instant) value).toEpochMilli() * 1000);
                        } else if (value instanceof OffsetDateTime) {
                            record.put(field.name(), ((OffsetDateTime) value).toInstant().toEpochMilli() * 1000);
                        }
                        break;
                    }
                    case "local-timestamp-millis": {
                        if (value instanceof LocalDateTime) {
                            long millis = ((LocalDateTime) value)
                                    .atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
                            record.put(field.name(), millis);
                        } else if (value instanceof OffsetDateTime) {
                            long millis = ((OffsetDateTime) value).toInstant().toEpochMilli();
                            record.put(field.name(), millis);
                        }
                        break;
                    }
                    case "local-timestamp-micros": {
                        if (value instanceof LocalDateTime) {
                            long micros = ((LocalDateTime) value)
                                    .atZone(ZoneOffset.UTC).toInstant().toEpochMilli() * 1000;
                            record.put(field.name(), micros);
                        } else if (value instanceof OffsetDateTime) {
                            long micros = ((OffsetDateTime) value).toInstant().toEpochMilli() * 1000;
                            record.put(field.name(), micros);
                        }
                        break;
                    }
                    case "decimal": {
                        if (value instanceof BigDecimal) {
                            LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) logicalType;
                            Conversions.DecimalConversion conversion = new Conversions.DecimalConversion();
                            record.put(field.name(),
                                    conversion.toBytes((BigDecimal) value, fieldSchema, decimalType));
                        }
                        break;
                    }
                    case "uuid": {
                        record.put(field.name(), value.toString());
                        break;
                    }
                    default: {
                        record.put(field.name(), value);
                        break;
                    }
                }
            } else {
                record.put(field.name(), value);
            }
        }
        return record;
    }

    private static Schema unwrapUnion(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            for (Schema s : schema.getTypes()) {
                if (s.getType() != Schema.Type.NULL) {
                    return s;
                }
            }
            throw new IllegalArgumentException("Union without non-null type: " + schema);
        }
        return schema;
    }
}