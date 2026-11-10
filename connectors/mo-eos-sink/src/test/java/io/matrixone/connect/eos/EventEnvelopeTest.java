package io.matrixone.connect.eos;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

final class EventEnvelopeTest {
    private static final byte[] SCOPE = "orders".getBytes(StandardCharsets.UTF_8);

    private static EventEnvelope parse(String json) {
        return EventEnvelope.parse(json.getBytes(StandardCharsets.UTF_8), SCOPE);
    }

    @Test
    void independentlyComputedFrameVectors() {
        EventEnvelope negative = parse("{\"version\":1,\"event_id\":\"A\","
            + "\"operation\":\"increment\",\"delta\":-7,\"payload\":\"提交确认😀\"}");
        assertEquals("37d223ab93c9d42f117a70bb7e0701e94c028c25459b5877d2f770a5b180049c",
            negative.digest);
        EventEnvelope escaped = parse("{\"payload\":\"\\u63d0\\u4ea4确认\\ud83d\\ude00\","
            + "\"delta\":-7,\"operation\":\"increment\",\"event_id\":\"A\",\"version\":1}");
        assertEquals(negative.digest, escaped.digest);
        assertEquals("1c743f578ca50afa1ba94769b5834b58355575139d8a18215699dfdf913a3cfd",
            parse("{\"version\":1,\"event_id\":\"max\",\"operation\":\"increment\","
                + "\"delta\":9223372036854775807,\"payload\":\"\"}").digest);
        assertEquals("d9e0dd41f07331a6e2983ce68141d5542ba2f9811b0ffccefaab05096120b0b6",
            parse("{\"version\":1,\"event_id\":\"A \",\"operation\":\"increment\","
                + "\"delta\":0,\"payload\":\"x\"}").digest);
        assertEquals("68d65ddf1eb7c73c6c64ef55891b292470e205e77ea0d3cdacaf9e7eb411ffac",
            parse("{\"version\":1,\"event_id\":\"a\",\"operation\":\"increment\","
                + "\"delta\":0,\"payload\":\"x\"}").digest);
        assertNotEquals(negative.digest,
            parse("{\"version\":1,\"event_id\":\"A\",\"operation\":\"increment\","
                + "\"delta\":-8,\"payload\":\"提交确认😀\"}").digest);
    }

    @Test
    void malformedAndUnsupportedRecordsFailClosed() {
        for (String json : new String[] {
            "{}",
            "{\"version\":1,\"event_id\":\"\",\"operation\":\"increment\",\"delta\":1,\"payload\":\"x\"}",
            "{\"version\":1,\"event_id\":\"x\",\"operation\":\"delete\",\"delta\":1,\"payload\":\"x\"}",
            "{\"version\":1,\"event_id\":\"x\",\"operation\":\"increment\",\"delta\":1.0,\"payload\":\"x\"}",
            "{\"version\":1,\"event_id\":\"x\",\"operation\":\"increment\",\"delta\":1e0,\"payload\":\"x\"}",
            "{\"version\":1,\"event_id\":\"x\",\"operation\":\"increment\",\"delta\":9223372036854775808,\"payload\":\"x\"}",
            "{\"version\":1,\"event_id\":\"x\",\"event_id\":\"y\",\"operation\":\"increment\",\"delta\":1,\"payload\":\"x\"}",
            "{\"version\":1,\"event_id\":\"x\",\"operation\":\"increment\",\"delta\":1,\"payload\":\"\\ud800\"}",
            "{\"version\":1,\"event_id\":\"x\",\"operation\":\"increment\",\"delta\":1,\"payload\":\"x\",\"extra\":0}",
        }) {
            assertThrows(IllegalArgumentException.class, () -> parse(json), json);
        }
        assertThrows(IllegalArgumentException.class, () -> EventEnvelope.parse(null, SCOPE));
        assertThrows(IllegalArgumentException.class, () -> EventEnvelope.parse(
            new byte[] {(byte) 0xff}, SCOPE));
    }
}
