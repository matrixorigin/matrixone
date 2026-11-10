package io.matrixone.connect.eos;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Set;

/** Strict version-one event envelope and byte-exact business digest. */
final class EventEnvelope {
    static final String DIGEST_VERSION = "mo-event-frame-v1";
    private static final Set<String> FIELDS = Set.of("version", "event_id", "operation", "delta", "payload");
    private static final ObjectMapper MAPPER = new ObjectMapper(
        JsonFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build())
        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    final String eventId;
    final byte[] eventIdBytes;
    final long delta;
    final String digest;

    private EventEnvelope(String eventId, byte[] eventIdBytes, long delta, String digest) {
        this.eventId = eventId;
        this.eventIdBytes = eventIdBytes;
        this.delta = delta;
        this.digest = digest;
    }

    static EventEnvelope parse(Object value, byte[] scope) {
        if (!(value instanceof byte[] bytes)) {
            throw new IllegalArgumentException("Kafka value must be bytes; configure ByteArrayConverter");
        }
        if (bytes.length > 1_100_000) {
            throw new IllegalArgumentException("event envelope exceeds byte limit");
        }
        try {
            String json = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes)).toString();
            JsonNode node = MAPPER.readTree(json);
            if (node == null || !node.isObject() || node.size() != FIELDS.size()) {
                throw new IllegalArgumentException("event must have exactly five fields");
            }
            for (String name : FIELDS) {
                if (!node.has(name)) {
                    throw new IllegalArgumentException("missing event field " + name);
                }
            }
            node.fieldNames().forEachRemaining(name -> {
                if (!FIELDS.contains(name)) {
                    throw new IllegalArgumentException("unknown event field " + name);
                }
            });
            JsonNode version = node.get("version");
            if (!version.isIntegralNumber() || !version.canConvertToInt() || version.intValue() != 1) {
                throw new IllegalArgumentException("unsupported event version");
            }
            String eventId = exactString(node.get("event_id"), "event_id");
            byte[] idBytes = eventId.getBytes(StandardCharsets.UTF_8);
            if (idBytes.length == 0 || idBytes.length > 256) {
                throw new IllegalArgumentException("event_id must be 1..256 UTF-8 bytes");
            }
            String operation = exactString(node.get("operation"), "operation");
            if (!"increment".equals(operation)) {
                throw new IllegalArgumentException("unsupported operation");
            }
            JsonNode deltaNode = node.get("delta");
            if (!deltaNode.isIntegralNumber() || !deltaNode.canConvertToLong()) {
                throw new IllegalArgumentException("delta must be an exact signed int64 integer");
            }
            long delta = deltaNode.longValue();
            String payload = exactString(node.get("payload"), "payload");
            if (payload.getBytes(StandardCharsets.UTF_8).length > 1_048_576) {
                throw new IllegalArgumentException("payload exceeds 1 MiB");
            }
            return new EventEnvelope(eventId, idBytes, delta,
                digest(scope, idBytes, operation, payload, delta));
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("invalid UTF-8 event envelope", e);
        } catch (IOException e) {
            throw new IllegalArgumentException("malformed JSON event envelope", e);
        }
    }

    private static String exactString(JsonNode value, String name) {
        if (!value.isTextual()) {
            throw new IllegalArgumentException(name + " must be a string");
        }
        String text = value.textValue();
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (Character.isHighSurrogate(c)) {
                if (++i >= text.length() || !Character.isLowSurrogate(text.charAt(i))) {
                    throw new IllegalArgumentException(name + " has an unpaired surrogate");
                }
            } else if (Character.isLowSurrogate(c)) {
                throw new IllegalArgumentException(name + " has an unpaired surrogate");
            }
        }
        return text;
    }

    private static String digest(byte[] scope, byte[] id, String operation,
                                 String payload, long delta) {
        try {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buffer);
            out.write(new byte[] {'M', 'O', 'E', '1'});
            writePart(out, scope);
            writePart(out, id);
            writePart(out, operation.getBytes(StandardCharsets.UTF_8));
            writePart(out, payload.getBytes(StandardCharsets.UTF_8));
            out.writeLong(delta);
            out.flush();
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(buffer.toByteArray()));
        } catch (IOException | NoSuchAlgorithmException e) {
            throw new IllegalStateException("cannot compute event digest", e);
        }
    }

    private static void writePart(DataOutputStream out, byte[] value) throws IOException {
        out.writeInt(value.length);
        out.write(value);
    }
}
