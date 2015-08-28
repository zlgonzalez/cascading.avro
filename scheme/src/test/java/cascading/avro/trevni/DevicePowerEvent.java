/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package cascading.avro.trevni;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class DevicePowerEvent extends org.apache.avro.specific.SpecificRecordBase implements
        org.apache.avro.specific.SpecificRecord {
    public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser()
            .parse("{\"type\":\"record\",\"name\":\"DevicePowerEvent\",\"namespace\":\"cascading.avro.trevni\",\"fields\":[{\"name\":\"power\",\"type\":\"double\"},{\"name\":\"deviceType\",\"type\":\"int\"},{\"name\":\"deviceId\",\"type\":\"int\"},{\"name\":\"status\",\"type\":\"int\"}]}");

    public static org.apache.avro.Schema getClassSchema() {
        return SCHEMA$;
    }

    @Deprecated
    public double power;
    @Deprecated
    public int deviceType;
    @Deprecated
    public int deviceId;
    @Deprecated
    public int status;

    /**
     * Default constructor.
     */
    public DevicePowerEvent() {
    }

    /**
     * All-args constructor.
     */
    public DevicePowerEvent(java.lang.Double power, java.lang.Integer deviceType, java.lang.Integer deviceId,
            java.lang.Integer status) {
        this.power = power;
        this.deviceType = deviceType;
        this.deviceId = deviceId;
        this.status = status;
    }

    public org.apache.avro.Schema getSchema() {
        return SCHEMA$;
    }

    // Used by DatumWriter. Applications should not call.
    public java.lang.Object get(int field$) {
        switch (field$) {
        case 0:
            return power;
        case 1:
            return deviceType;
        case 2:
            return deviceId;
        case 3:
            return status;
        default:
            throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    // Used by DatumReader. Applications should not call.
    @SuppressWarnings(value = "unchecked")
    public void put(int field$, java.lang.Object value$) {
        switch (field$) {
        case 0:
            power = (java.lang.Double) value$;
            break;
        case 1:
            deviceType = (java.lang.Integer) value$;
            break;
        case 2:
            deviceId = (java.lang.Integer) value$;
            break;
        case 3:
            status = (java.lang.Integer) value$;
            break;
        default:
            throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    /**
     * Gets the value of the 'power' field.
     */
    public java.lang.Double getPower() {
        return power;
    }

    /**
     * Sets the value of the 'power' field.
     * 
     * @param value
     *            the value to set.
     */
    public void setPower(java.lang.Double value) {
        this.power = value;
    }

    /**
     * Gets the value of the 'deviceType' field.
     */
    public java.lang.Integer getDeviceType() {
        return deviceType;
    }

    /**
     * Sets the value of the 'deviceType' field.
     * 
     * @param value
     *            the value to set.
     */
    public void setDeviceType(java.lang.Integer value) {
        this.deviceType = value;
    }

    /**
     * Gets the value of the 'deviceId' field.
     */
    public java.lang.Integer getDeviceId() {
        return deviceId;
    }

    /**
     * Sets the value of the 'deviceId' field.
     * 
     * @param value
     *            the value to set.
     */
    public void setDeviceId(java.lang.Integer value) {
        this.deviceId = value;
    }

    /**
     * Gets the value of the 'status' field.
     */
    public java.lang.Integer getStatus() {
        return status;
    }

    /**
     * Sets the value of the 'status' field.
     * 
     * @param value
     *            the value to set.
     */
    public void setStatus(java.lang.Integer value) {
        this.status = value;
    }

    /** Creates a new DevicePowerEvent RecordBuilder */
    public static cascading.avro.trevni.DevicePowerEvent.Builder newBuilder() {
        return new cascading.avro.trevni.DevicePowerEvent.Builder();
    }

    /**
     * Creates a new DevicePowerEvent RecordBuilder by copying an existing
     * Builder
     */
    public static cascading.avro.trevni.DevicePowerEvent.Builder newBuilder(
            cascading.avro.trevni.DevicePowerEvent.Builder other) {
        return new cascading.avro.trevni.DevicePowerEvent.Builder(other);
    }

    /**
     * Creates a new DevicePowerEvent RecordBuilder by copying an existing
     * DevicePowerEvent instance
     */
    public static cascading.avro.trevni.DevicePowerEvent.Builder newBuilder(cascading.avro.trevni.DevicePowerEvent other) {
        return new cascading.avro.trevni.DevicePowerEvent.Builder(other);
    }

    /**
     * RecordBuilder for DevicePowerEvent instances.
     */
    public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<DevicePowerEvent> implements
            org.apache.avro.data.RecordBuilder<DevicePowerEvent> {

        private double power;
        private int deviceType;
        private int deviceId;
        private int status;

        /** Creates a new Builder */
        private Builder() {
            super(cascading.avro.trevni.DevicePowerEvent.SCHEMA$);
        }

        /** Creates a Builder by copying an existing Builder */
        private Builder(cascading.avro.trevni.DevicePowerEvent.Builder other) {
            super(other);
        }

        /** Creates a Builder by copying an existing DevicePowerEvent instance */
        private Builder(cascading.avro.trevni.DevicePowerEvent other) {
            super(cascading.avro.trevni.DevicePowerEvent.SCHEMA$);
            if (isValidValue(fields()[0], other.power)) {
                this.power = data().deepCopy(fields()[0].schema(), other.power);
                fieldSetFlags()[0] = true;
            }
            if (isValidValue(fields()[1], other.deviceType)) {
                this.deviceType = data().deepCopy(fields()[1].schema(), other.deviceType);
                fieldSetFlags()[1] = true;
            }
            if (isValidValue(fields()[2], other.deviceId)) {
                this.deviceId = data().deepCopy(fields()[2].schema(), other.deviceId);
                fieldSetFlags()[2] = true;
            }
            if (isValidValue(fields()[3], other.status)) {
                this.status = data().deepCopy(fields()[3].schema(), other.status);
                fieldSetFlags()[3] = true;
            }
        }

        /** Gets the value of the 'power' field */
        public java.lang.Double getPower() {
            return power;
        }

        /** Sets the value of the 'power' field */
        public cascading.avro.trevni.DevicePowerEvent.Builder setPower(double value) {
            validate(fields()[0], value);
            this.power = value;
            fieldSetFlags()[0] = true;
            return this;
        }

        /** Checks whether the 'power' field has been set */
        public boolean hasPower() {
            return fieldSetFlags()[0];
        }

        /** Clears the value of the 'power' field */
        public cascading.avro.trevni.DevicePowerEvent.Builder clearPower() {
            fieldSetFlags()[0] = false;
            return this;
        }

        /** Gets the value of the 'deviceType' field */
        public java.lang.Integer getDeviceType() {
            return deviceType;
        }

        /** Sets the value of the 'deviceType' field */
        public cascading.avro.trevni.DevicePowerEvent.Builder setDeviceType(int value) {
            validate(fields()[1], value);
            this.deviceType = value;
            fieldSetFlags()[1] = true;
            return this;
        }

        /** Checks whether the 'deviceType' field has been set */
        public boolean hasDeviceType() {
            return fieldSetFlags()[1];
        }

        /** Clears the value of the 'deviceType' field */
        public cascading.avro.trevni.DevicePowerEvent.Builder clearDeviceType() {
            fieldSetFlags()[1] = false;
            return this;
        }

        /** Gets the value of the 'deviceId' field */
        public java.lang.Integer getDeviceId() {
            return deviceId;
        }

        /** Sets the value of the 'deviceId' field */
        public cascading.avro.trevni.DevicePowerEvent.Builder setDeviceId(int value) {
            validate(fields()[2], value);
            this.deviceId = value;
            fieldSetFlags()[2] = true;
            return this;
        }

        /** Checks whether the 'deviceId' field has been set */
        public boolean hasDeviceId() {
            return fieldSetFlags()[2];
        }

        /** Clears the value of the 'deviceId' field */
        public cascading.avro.trevni.DevicePowerEvent.Builder clearDeviceId() {
            fieldSetFlags()[2] = false;
            return this;
        }

        /** Gets the value of the 'status' field */
        public java.lang.Integer getStatus() {
            return status;
        }

        /** Sets the value of the 'status' field */
        public cascading.avro.trevni.DevicePowerEvent.Builder setStatus(int value) {
            validate(fields()[3], value);
            this.status = value;
            fieldSetFlags()[3] = true;
            return this;
        }

        /** Checks whether the 'status' field has been set */
        public boolean hasStatus() {
            return fieldSetFlags()[3];
        }

        /** Clears the value of the 'status' field */
        public cascading.avro.trevni.DevicePowerEvent.Builder clearStatus() {
            fieldSetFlags()[3] = false;
            return this;
        }

        @Override
        public DevicePowerEvent build() {
            try {
                DevicePowerEvent record = new DevicePowerEvent();
                record.power = fieldSetFlags()[0] ? this.power : (java.lang.Double) defaultValue(fields()[0]);
                record.deviceType = fieldSetFlags()[1] ? this.deviceType
                        : (java.lang.Integer) defaultValue(fields()[1]);
                record.deviceId = fieldSetFlags()[2] ? this.deviceId : (java.lang.Integer) defaultValue(fields()[2]);
                record.status = fieldSetFlags()[3] ? this.status : (java.lang.Integer) defaultValue(fields()[3]);
                return record;
            } catch (Exception e) {
                throw new org.apache.avro.AvroRuntimeException(e);
            }
        }
    }
}
