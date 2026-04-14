package io.quarkus.reactivemessaging.http.sink.app;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;

import org.codehaus.plexus.util.StringUtils;

@ApplicationScoped
@Path("/processing-endpoint")
public class HttpProcessingEndpoint {

    private static boolean isTested = true;

    private List<BlockingQueue<String>> queues = new CopyOnWriteArrayList<>();
    private ReadWriteLock consumptionLock = new ReentrantReadWriteLock();

    @POST
    @Produces(MediaType.TEXT_PLAIN)
    public Response handlePost() throws IOException {

        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        queue.offer("RCV");

        queues.add(queue);

        try {
            consumptionLock.readLock().lock();
        } finally {
            consumptionLock.readLock().unlock();
        }

        StreamingOutput streamingOutput = output -> {
            while (isTested) {
                try {
                    String chunk = queue.poll(200, TimeUnit.MILLISECONDS);
                    if ("DONE".equals(chunk)) {
                        break;
                    }
                    if (StringUtils.isNotEmpty(chunk)) {
                        output.write(chunk.getBytes());
                        output.flush();
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }
        };

        return Response.ok(streamingOutput)
                .status(202)
                .build();
    }

    public int getSize() {
        return queues.size();
    }

    public void ackAll() {
        for (BlockingQueue<String> queue : queues) {
            queue.offer("ACK");
        }
    }

    public void nackAll() {
        for (BlockingQueue<String> queue : queues) {
            queue.offer("NACK");
        }
    }

    public void closeStreams() {
        for (BlockingQueue<String> queue : queues) {
            queue.offer("DONE");
        }
    }

    public void clearList() {
        queues.clear();
    }

    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    public void pause() {
        consumptionLock.writeLock().lock();
    }

    public void resume() {
        consumptionLock.writeLock().unlock();
    }

    public void turnOffTestLoop() {
        isTested = false;
    }

    public void turnOnTestLoop() {
        isTested = true;
    }

}
