package io.quarkus.reactivemessaging.http.runtime;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

import io.netty.handler.codec.http.QueryStringEncoder;
import io.quarkus.reactivemessaging.http.runtime.config.TlsConfig;
import io.quarkus.reactivemessaging.http.runtime.serializers.Serializer;
import io.quarkus.reactivemessaging.http.runtime.serializers.SerializerFactoryBase;
import io.quarkus.tls.TlsConfiguration;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.http.RequestOptions;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.core.http.HttpClient;
import io.vertx.mutiny.core.http.HttpClientRequest;
import io.vertx.mutiny.core.http.HttpClientResponse;

class HttpSink extends AbstractSink {

    private static final Logger log = Logger.getLogger(HttpSink.class);

    private static final String[] SUPPORTED_SCHEMES = { "http:", "https:" };

    private final HttpClient httpClient;
    private final String method;
    private final String url;
    private final SerializerFactoryBase serializerFactory;
    private final String serializerName;

    HttpSink(Vertx vertx, String method, String url,
            String serializerName,
            int maxRetries,
            double jitter,
            Optional<Duration> delay,
            Optional<Integer> maxPoolSize,
            Optional<Integer> maxWaitQueueSize,
            SerializerFactoryBase serializerFactory,
            Optional<TlsConfiguration> tlsConfiguration,
            long inflights,
            boolean waitForCompletion,
            HttpVersion protocolVersion,
            boolean twoPhaseResponseFlow) {
        super(log, url, maxRetries, jitter, delay, inflights, waitForCompletion, twoPhaseResponseFlow);
        this.method = method;
        this.url = url;
        this.serializerFactory = serializerFactory;
        this.serializerName = serializerName;

        HttpClientOptions options = new HttpClientOptions();
        maxPoolSize.ifPresent(options::setMaxPoolSize);
        maxWaitQueueSize.ifPresent(options::setMaxWaitQueueSize);

        tlsConfiguration.ifPresent(config -> TlsConfig.configure(options, config));

        options.setProtocolVersion(protocolVersion);
        if (protocolVersion == HttpVersion.HTTP_2 && tlsConfiguration.isPresent()) {
            options.setUseAlpn(true);
        }

        this.httpClient = new HttpClient(vertx.createHttpClient(options));

        if (Arrays.stream(SUPPORTED_SCHEMES).noneMatch(url.toLowerCase()::startsWith)) {
            throw new IllegalArgumentException("Unsupported scheme for the http connector in URL: " + url);
        }
    }

    @Override
    protected Uni<Void> send(Message<?> message) {
        Uni<HttpClientRequest> request = toHttpRequest(message);

        return request.onItem()
                .transformToUni(req -> invoke(message, req, serialize(message.getPayload())));
    }

    private <T> Buffer serialize(T payload) {
        Serializer<T> serializer = serializerFactory.getSerializer(serializerName, payload);
        return Buffer.newInstance(serializer.serialize(payload));
    }

    private Uni<Void> invoke(Message<?> message, HttpClientRequest request, Buffer buffer) {
        log.debugf("Invoking request: ", toString(request, buffer));
        return request.send(buffer).onItem().transform(response -> {
            if (this.twoPhaseResponseFlow) {
                response
                        .toMulti()
                        .subscribe().with(item -> handleBuffer(message, request, response, item),
                                message::nack, () -> {
                                });
            }

            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return null;
            } else {
                throw new VertxException(
                        "Http request: " + toString(request) + " failed with response: " + toString(response));
            }
        });
    }

    private void handleBuffer(Message<?> message, HttpClientRequest request,
            HttpClientResponse response, Buffer buf) {
        String body = buf.toString().strip();
        if (body.equals("NACK")) {
            message.nack(new VertxException(
                    "Http request: " + toString(request) + " failed with response: " + toString(
                            response)));
        } else if (body.equals("ACK")) {
            message.ack();
        } else if (body.equals("RCV")) {
            // skip, this is our body
        } else {
            String logMessage = "Http request: " + toString(request) + " returned unexpected body: [" + buf + "] for response:"
                    + toString(response);
            message.nack(new VertxException(logMessage));
            log.warnf(logMessage);
        }
    }

    private String toString(HttpClientRequest req) {
        return "URI:" + req.getURI() + " Method:" + req.getMethod() + " Headers: " + req.headers();
    }

    private String toString(HttpClientRequest req, Buffer buffer) {
        return toString(req) + " Body: " + buffer;
    }

    private String toString(HttpClientResponse resp) {
        return "Code: " + resp.statusCode() + " Message: " + resp.statusMessage();
    }

    private Uni<HttpClientRequest> toHttpRequest(Message<?> message) {
        try {
            OutgoingHttpMetadata metadata = message.getMetadata(OutgoingHttpMetadata.class).orElse((OutgoingHttpMetadata) null);

            Map<String, String> cloudEventHeaders = HttpCloudEventHelper.getCloudEventHeaders(message);
            Map<String, List<String>> httpHeaders = metadata != null ? metadata.getHeaders() : Collections.emptyMap();
            httpHeaders = safeAddAll(httpHeaders, cloudEventHeaders);

            Map<String, List<String>> query = metadata != null ? metadata.getQuery() : Collections.emptyMap();
            Map<String, String> pathParams = metadata != null ? metadata.getPathParameters() : Collections.emptyMap();

            String url = prepareUrl(pathParams);

            Uni<HttpClientRequest> request = createRequest(url);

            Map<String, List<String>> finalHttpHeaders = httpHeaders;
            return request
                    .onItem().transform(req -> {
                        addHeaders(req, finalHttpHeaders);
                        addQueryParameters(query, req);

                        return req;
                    });
        } catch (Exception any) {
            log.error("Failed to transform message to http request", any);
            throw any;
        }
    }

    private Map<String, List<String>> safeAddAll(Map<String, List<String>> httpHeaders,
            Map<String, String> cloudEventHeaders) {
        if (cloudEventHeaders.isEmpty()) {
            return httpHeaders;
        } else if (httpHeaders.isEmpty()) {
            return cloudEventHeaders.entrySet().stream()
                    .collect(Collectors.toMap(Entry::getKey, e -> List.<String> of(e.getValue())));
        } else {
            // httpHeaders might be inmutable
            Map<String, List<String>> mergedMap = new HashMap<>(httpHeaders);
            for (Entry<String, String> entry : cloudEventHeaders.entrySet()) {
                mergedMap.put(entry.getKey(), List.of(entry.getValue()));
            }
            return mergedMap;
        }
    }

    private Uni<HttpClientRequest> createRequest(String url) {
        RequestOptions options = new RequestOptions()
                .setAbsoluteURI(url);
        switch (method) {
            case "POST" -> options.setMethod(HttpMethod.POST);
            case "PUT" -> options.setMethod(HttpMethod.PUT);
            default ->
                throw new IllegalArgumentException("Unsupported HTTP method: " + method + " only PUT and POST are supported");
        }

        return httpClient.request(options);
    }

    private void addQueryParameters(Map<String, List<String>> query, HttpClientRequest request) {
        if (query == null || query.isEmpty()) {
            return;
        }

        QueryStringEncoder encoder = new QueryStringEncoder(request.getURI());

        for (Entry<String, List<String>> entry : query.entrySet()) {
            String key = entry.getKey();
            for (String value : entry.getValue()) {
                encoder.addParam(key, value);
            }
        }

        request.setURI(encoder.toString());
    }

    private void addHeaders(HttpClientRequest request, Map<String, List<String>> httpHeaders) {
        if (!httpHeaders.isEmpty()) {
            for (Map.Entry<String, List<String>> header : httpHeaders.entrySet()) {
                request.putHeader(header.getKey(), header.getValue());
            }
        }
    }

    private String prepareUrl(Map<String, String> pathParams) {
        String result = url;
        for (Map.Entry<String, String> pathParamEntry : pathParams.entrySet()) {
            String toReplace = String.format("{%s}", pathParamEntry.getKey());
            if (url.contains(toReplace)) {
                result = url.replace(toReplace, pathParamEntry.getValue());
            } else {
                log.warnf("Failed to find %s in the URL that would correspond to the %s path parameter",
                        toReplace, pathParamEntry.getKey());
            }
        }

        return result;
    }
}
