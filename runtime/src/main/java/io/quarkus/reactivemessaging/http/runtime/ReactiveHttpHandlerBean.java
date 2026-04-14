package io.quarkus.reactivemessaging.http.runtime;

import java.util.Collection;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.jboss.logging.Logger;

import io.quarkus.reactivemessaging.http.runtime.config.HttpStreamConfig;
import io.quarkus.reactivemessaging.http.runtime.config.ReactiveHttpConfig;
import io.quarkus.reactivemessaging.http.runtime.serializers.DeserializerFactoryBase;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.MultiEmitter;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.RoutingContext;

/**
 * a bean that handles incoming http requests
 */
@Singleton
public class ReactiveHttpHandlerBean extends ReactiveHandlerBeanBase<HttpStreamConfig, HttpMessage<?>> {

    private static final Logger log = Logger.getLogger(ReactiveHttpHandlerBean.class);

    @Inject
    ReactiveHttpConfig config;

    @Inject
    DeserializerFactoryBase deserializerFactory;

    Multi<HttpMessage<?>> getProcessor(String path, HttpMethod method) {
        return processors.get(key(path, method)).getProcessor();
    }

    @Override
    protected Collection<HttpStreamConfig> configs() {
        return config.getHttpConfigs();
    }

    @Override
    protected String key(HttpStreamConfig streamConfig) {
        return key(streamConfig.path, streamConfig.method);
    }

    @Override
    protected String key(RoutingContext context) {
        return key(context.currentRoute().getPath(), context.request().method());
    }

    @Override
    protected String description(HttpStreamConfig streamConfig) {
        return String.format("path: %s, method %s", streamConfig.path, streamConfig.method);
    }

    @Override
    protected void handleRequest(RoutingContext event, MultiEmitter<? super HttpMessage<?>> emitter,
            StrictQueueSizeGuard guard, String path, String deserializerName, boolean twoPhaseResponseFlow) {
        if (emitter == null) {
            onUnexpectedError(event, twoPhaseResponseFlow, null,
                    "No consumer subscribed for messages sent to Reactive Messaging HTTP endpoint on path: " + path);
        } else if (guard.prepareToEmit()) {
            try {
                if (twoPhaseResponseFlow) {
                    guard.putInQueue(() -> statusEvent(event));
                }
                emitter.emit(new HttpMessage<>(
                        deserializerFactory.getDeserializer(deserializerName)
                                .map(d -> d.deserialize(event.body().buffer()))
                                .orElse(event.body().buffer()),
                        new IncomingHttpMetadata(event),
                        () -> {
                            ackEvent(event, twoPhaseResponseFlow);
                        },
                        error -> onUnexpectedError(event, twoPhaseResponseFlow, error, "Failed to process message")));
            } catch (Exception any) {
                guard.dequeue();
                onUnexpectedError(event, twoPhaseResponseFlow, any, "Emitting message failed");
            }
        } else {
            nackEvent(event, twoPhaseResponseFlow);
        }
    }

    private void onUnexpectedError(RoutingContext event, boolean twoPhaseResponseFlow, Throwable error, String message) {
        nackEvent(event, twoPhaseResponseFlow);
        log.error(message + (error != null ? ": " + error.getMessage() : ""));
        log.debug(message, error);
    }

    protected void ackEvent(RoutingContext event, boolean twoPhaseResponseFlow) {
        if (twoPhaseResponseFlow) {
            if (!event.response().ended()) {
                if (event.response().getStatusCode() == 200) {
                    event.response().setStatusCode(202);
                }
                event.response().end("ACK");
            }
        } else {
            if (!event.response().ended()) {
                event.response().setStatusCode(202).end();
            }
        }
    }

    protected void nackEvent(RoutingContext event, boolean twoPhaseResponseFlow) {
        if (twoPhaseResponseFlow) {
            if (!event.response().ended()) {
                if (event.response().getStatusCode() == 200) {
                    event.response().setStatusCode(503);
                }
                event.response().end("NACK");
            }
        } else {
            if (!event.response().ended()) {
                event.response().setStatusCode(503).end();
            }
        }
    }

    protected void statusEvent(RoutingContext event) {
        if (!event.response().ended()) {
            if (event.response().getStatusCode() == 200) {
                event.response().setStatusCode(202);
            }
            if (!event.response().isChunked()) {
                event.response().setChunked(true);
            }
            event.response().write("RCV");
        }
    }

    private String key(String path, HttpMethod method) {
        return String.format("%s:%s", path, method);
    }
}
