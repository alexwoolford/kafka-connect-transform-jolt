package io.woolford;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class JoltTransformTest {

    private static final Logger log = LoggerFactory.getLogger(JoltTransformTest.class);
    private final JoltTransform.Value<SinkRecord> xformValue = new JoltTransform.Value<>();


    private SinkRecord getSinkRecord() throws IOException {

        String recordString = readFile("src/test/resources/record-1.json", StandardCharsets.US_ASCII);
        ObjectMapper mapper = new ObjectMapper();

        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {};
        Map<String, Object> recordValue = mapper.readValue(recordString, typeRef);

        return new SinkRecord(
                "test",
                0,
                null,
                null,
                null,
                recordValue,
                0
        );

    }


    @Test
    public void joltTransformTest() throws IOException {

        String joltSpec = readFile("src/test/resources/spec.json", StandardCharsets.US_ASCII);

        xformValue.configure(new HashMap<String, Object>(){{
            put(JoltTransform.JOLT_SPEC_CONFIG, joltSpec);
        }});

        SinkRecord record = getSinkRecord();

        SinkRecord transformedRecord = xformValue.apply(record);

        ObjectMapper mapper = new ObjectMapper();
        String transformedRecordJson = mapper.writeValueAsString(transformedRecord.value());

        Assert.assertEquals("{\"location\":{\"lon\":-84.2951,\"lat\":33.7657},\"ts\":1603161082983,\"ip_orig\":\"10.0.1.61\",\"ip_resp\":\"50.205.244.36\",\"uid\":\"CslSx83mieZIJISXql\"}", transformedRecordJson);

    }

    static String readFile(String path, Charset encoding)
            throws IOException
    {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }

    String test = "[{\"operation\":\"shift\",\"spec\":{\"RESP_LOCATION\":{\"LOCATION\":{\"LON\":\"lon\",\"LAT\":\"lat\"}},\"ID_ORIG_H\":\"ip_orig\",\"TS\":\"ts\",\"ID_RESP_H\":\"ip_resp\",\"UID\":\"uid\"}},{\"operation\":\"shift\",\"spec\":{\"lon\":\"location.lon\",\"lat\":\"location.lat\",\"ts\":\"ts\",\"ip_orig\":\"ip_orig\",\"ip_resp\":\"ip_resp\",\"uid\":\"uid\"}}]";

}
