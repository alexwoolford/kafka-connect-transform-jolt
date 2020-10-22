package io.woolford;

import com.bazaarvoice.jolt.Chainr;
import com.bazaarvoice.jolt.JsonUtils;
import io.woolford.util.SimpleConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.Transformation;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public abstract class JoltTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOG = LoggerFactory.getLogger(JoltTransform.class);

    // Specifies properties for configuration.
    public static final String JOLT_SPEC_CONFIG = "jolt.spec";
    public static final String JOLT_SPEC_DISPLAY = "Jolt specification";

    /**
     * Validate if the defined string is valid Jolt transformation.
     **/
    private static final Validator VALIDATOR_JOLT_SPEC = (name, value) -> {
        try {
            Chainr.fromSpec(JsonUtils.jsonToList(String.valueOf(value)));
        } catch (Exception e) {
            throw new ConfigException(name, value, "Invalid Jolt specification. Go to https://jolt-demo.appspot.com/ "
                    + "and use that to help craft your transformation.");
        }
    };

    private SimpleConfig config;
    private Chainr chainr;

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(JOLT_SPEC_CONFIG,
                    ConfigDef.Type.STRING,
//                    VALIDATOR_JOLT_SPEC, // TODO: fix validator
                    ConfigDef.Importance.HIGH,
                    "Jolt transform JSON",
                    null,
                    -1,
                    ConfigDef.Width.LONG,
                    JOLT_SPEC_DISPLAY);

    @Override
    public void configure(Map<String, ?> props) {
        config = new SimpleConfig(CONFIG_DEF, props);
        chainr = Chainr.fromSpec(JsonUtils.jsonToList(config.getString(JOLT_SPEC_CONFIG)));
    }

    @Override
    public R apply(R record) {
        Object transformedRecordValue = chainr.transform( record.value() );
        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                transformedRecordValue,
                record.timestamp()
        );
    }

    @Override
    public void close() {

    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    public static class Key<R extends ConnectRecord<R>> extends JoltTransform<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends JoltTransform<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }
    }

}
