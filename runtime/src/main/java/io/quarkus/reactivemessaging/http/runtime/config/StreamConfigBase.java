package io.quarkus.reactivemessaging.http.runtime.config;

public class StreamConfigBase {
    public final int bufferSize;
    public final String path;
    public final String deserializerName;
    public final boolean twoPhaseResponseFlow;

    public StreamConfigBase(int bufferSize, String path, String deserializerName, boolean twoPhaseResponseFlow) {
        this.path = path;
        this.bufferSize = bufferSize;
        this.deserializerName = deserializerName;
        this.twoPhaseResponseFlow = twoPhaseResponseFlow;
    }
}
