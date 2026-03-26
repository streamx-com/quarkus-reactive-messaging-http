package io.quarkus.reactivemessaging.http.runtime;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

class WebSocketMessage<PayloadType> implements Message<PayloadType> {

    private final PayloadType payload;
    private final Supplier<CompletionStage<Void>> ackHandler;
    private final Function<Throwable, CompletionStage<Void>> nackHandler;
    private final Metadata metadata;

    WebSocketMessage(PayloadType payload, RequestMetadata requestMetadata,
            Supplier<CompletionStage<Void>> ackHandler,
            Function<Throwable, CompletionStage<Void>> nackHandler) {
        this.payload = payload;
        this.ackHandler = ackHandler;
        this.nackHandler = nackHandler;
        metadata = Metadata.of(requestMetadata);
    }

    @Override
    public PayloadType getPayload() {
        return payload;
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public Supplier<CompletionStage<Void>> getAck() {
        return ackHandler;
    }

    @Override
    public Function<Throwable, CompletionStage<Void>> getNack() {
        return nackHandler;
    }
}
