package com.paynova.obs;

import brave.Tracing;
import brave.Tracer;
import brave.sampler.Sampler;
import zipkin2.reporter.okhttp3.OkHttpSender;
import zipkin2.reporter.AsyncReporter;

public class ZipkinTracer {
    private static Tracer tracer;

    public static Tracer init(String serviceName, String zipkinUrl) {
        AsyncReporter<zipkin2.Span> reporter = AsyncReporter.create(
                OkHttpSender.create(zipkinUrl + "/api/v2/spans"));
        Tracing tracing = Tracing.newBuilder()
                .localServiceName(serviceName)
                .spanReporter(reporter)
                .sampler(Sampler.ALWAYS_SAMPLE)
                .build();
        tracer = tracing.tracer();
        return tracer;
    }

    public static Tracer get() { return tracer; }
}