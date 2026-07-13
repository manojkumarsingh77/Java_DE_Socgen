package com.paynova.obs;

import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;

public class PrometheusServer {
    private static HTTPServer server;

    public static void start(int port) throws Exception {
        DefaultExports.initialize();
        server = new HTTPServer(port);
    }

    public static void stop() {
        if (server != null) server.stop();
    }
}