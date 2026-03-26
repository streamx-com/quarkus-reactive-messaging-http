package io.quarkus.reactivemessaging.http.runtime;

import java.util.List;
import java.util.Objects;

/**
 * Response send back to web socket after sending a message to signal ack/nack. Format is similar to
 * STOMP but simplified to ACK/NACK command + optional 'id' header.
 */
public class WebSocketResponse {

    public static final String EOL = "\n";
    public static final String ACK_COMMAND = "ACK";
    public static final String NACK_COMMAND = "NACK";
    public static final List<String> ALLOWED_COMMANDS = List.of(ACK_COMMAND, NACK_COMMAND);
    public static final String ID_HEADER_PREFIX = "id:";

    private final String command;
    private final String messageId;

    private WebSocketResponse(String command, String messageId) {
        this.command = Objects.requireNonNull(command);
        this.messageId = messageId;
    }

    public boolean isAck() {
        return ACK_COMMAND.equals(command);
    }

    public String getMessageId() {
        return messageId;
    }

    @Override
    public String toString() {
        return command + (messageId != null ? EOL + ID_HEADER_PREFIX + messageId : "");
    }

    public static WebSocketResponse ack(String messageId) {
        return new WebSocketResponse(ACK_COMMAND, messageId);
    }

    public static WebSocketResponse nack(String messageId) {
        return new WebSocketResponse(NACK_COMMAND, messageId);
    }

    public static WebSocketResponse parseResponseWithMessageId(String response) {
        if (response == null) {
            return null;
        }
        String[] parts = response.split(EOL);
        if (parts.length != 2) {
            return null;
        }
        String command = parts[0];
        if (!ALLOWED_COMMANDS.contains(command)) {
            return null;
        }
        String header = parts[1];
        if (!header.startsWith(ID_HEADER_PREFIX)) {
            return null;
        }
        String messageId = header.substring(ID_HEADER_PREFIX.length());
        if (messageId.isBlank()) {
            return null;
        }
        return new WebSocketResponse(command, messageId);
    }
}
