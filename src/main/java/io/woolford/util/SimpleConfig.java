package io.woolford.util;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

// Originally copied from the org.apache.kafka.connect.transforms.util.SimpleConfig class to avoid
// depending on non-public API and to enable customizations
public class SimpleConfig extends AbstractConfig {

    public SimpleConfig(ConfigDef config, Map<String, ?> props) {
        super(config, props, false);
    }
}
