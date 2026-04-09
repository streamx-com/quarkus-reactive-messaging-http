package io.quarkiverse.reactivemessaging.http.quickstart;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;

@Path("/cost-collector")
@ApplicationScoped
public class CostCollector {

    private double sum = 0;

    @POST
    public synchronized String consumeCost(String valueAsString) {
        sum += Double.parseDouble(valueAsString);
        return "ACK";
    }

    @GET
    public synchronized double getSum() {
        return sum;
    }

}
