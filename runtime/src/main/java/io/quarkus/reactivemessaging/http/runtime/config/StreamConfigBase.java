package io.quarkus.reactivemessaging.http.runtime.config;

public class StreamConfigBase {
    public final int bufferSize;
    public final String path;
    public final String deserializerName;
    public final boolean twoFaceResponseFlow;

    public StreamConfigBase(int bufferSize, String path, String deserializerName, boolean twoFaceResponseFlow) {
        this.path = path;
        this.bufferSize = bufferSize;
        this.deserializerName = deserializerName;
        this.twoFaceResponseFlow = twoFaceResponseFlow;
    }
}
