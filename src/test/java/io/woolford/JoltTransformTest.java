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


    private SinkRecord getSinkRecord1() throws IOException {

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

    private SinkRecord getSinkRecord2() throws IOException {

        String recordString = readFile("src/test/resources/record-2.json", StandardCharsets.US_ASCII);
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
    public void joltTransformSpec1Test() throws IOException {

        String joltSpec = readFile("src/test/resources/spec-1.json", StandardCharsets.US_ASCII);

        xformValue.configure(new HashMap<String, Object>(){{
            put(JoltTransform.JOLT_SPEC_CONFIG, joltSpec);
        }});

        SinkRecord record = getSinkRecord1();

        SinkRecord transformedRecord = xformValue.apply(record);

        ObjectMapper mapper = new ObjectMapper();
        String transformedRecordJson = mapper.writeValueAsString(transformedRecord.value());

        Assert.assertEquals("{\"location\":{\"lon\":-84.2951,\"lat\":33.7657},\"ts\":1603161082983,\"ip_orig\":\"10.0.1.61\",\"ip_resp\":\"50.205.244.36\",\"uid\":\"CslSx83mieZIJISXql\"}", transformedRecordJson);

    }


    @Test
    public void joltTransformSpec2Test() throws IOException {

        String joltSpec = readFile("src/test/resources/spec-2.json", StandardCharsets.US_ASCII);

        xformValue.configure(new HashMap<String, Object>(){{
            put(JoltTransform.JOLT_SPEC_CONFIG, joltSpec);
        }});

        SinkRecord record = getSinkRecord2();

        SinkRecord transformedRecord = xformValue.apply(record);

        ObjectMapper mapper = new ObjectMapper();
        String transformedRecordJson = mapper.writeValueAsString(transformedRecord.value());

        Assert.assertEquals("{\"ts\":1609300742514,\"uid\":\"C5gXW32wa2ez0o7P0h\",\"orig\":{\"location\":{\"lat\":39.997,\"lon\":-105.0974},\"city\":\"Lafayette\",\"region\":\"CO\",\"country_code\":\"US\"},\"resp\":{\"location\":{\"lat\":40.7449,\"lon\":-75.2217},\"city\":\"Easton\",\"region\":\"PA\",\"country_code\":\"US\"}}", transformedRecordJson);

    }

    static String readFile(String path, Charset encoding)
            throws IOException
    {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }

    String test = "[{\"operation\":\"shift\",\"spec\":{\"ts\":\"ts\",\"ip_orig_h\":\"ip_orig\",\"ip_resp_h\":\"ip_resp\",\"uid\":\"uid\",\"geo_orig_latitude\":\"orig.location.lat\",\"geo_orig_longitude\":\"orig.location.lon\",\"geo_orig_city\":\"orig.city\",\"geo_orig_region\":\"orig.region\",\"geo_orig_country_code\":\"orig.country_code\",\"geo_resp_latitude\":\"resp.location.lat\",\"geo_resp_longitude\":\"resp.location.lon\",\"geo_resp_city\":\"resp.city\",\"geo_resp_region\":\"resp.region\",\"geo_resp_country_code\":\"resp.country_code\"}}]";
}
