package io.rsocket.examples;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.exceptions.ConnectionCloseException;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import io.rsocket.util.EmptyPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

public class ReconnectExampleMain {
	static final Logger LOG = LoggerFactory.getLogger("ReconnectExample");

	public static void main(String[] args) {
		RSocketServer.create(SocketAcceptor.forRequestStream(p -> Flux.interval(Duration.ofSeconds(1))
		                                                              .log()
		                                                              .map(i -> DefaultPayload.create("Hello " + i))))
		             .bind(TcpServerTransport.create("localhost", 8000))
		             .subscribe();

		AtomicInteger totalRetries = new AtomicInteger();
		Mono<RSocket> connectionMono = RSocketConnector
				.create()
				.reconnect(
					Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(100))
						 .maxBackoff(Duration.ofMinutes(1))
					     .doBeforeRetry(rs -> LOG.error("Retrying to connect, failed with signal {}", rs))
						 .filter(t -> totalRetries.getAndIncrement() < 10)
				)
				.connect(TcpClientTransport.create("localhost", 8001));

		connectionMono
				.flatMap(rsocket -> {
					rsocket.requestStream(EmptyPayload.INSTANCE)
					       .subscribe();

					return rsocket.onClose().doOnError(t -> LOG.error("Connection " +
							"failed with error", t));
				})
				.retryWhen(Retry.indefinitely().filter(t -> totalRetries.getAndIncrement() < 10))
				.block();

		LOG.info("Terminated");
	}
}
