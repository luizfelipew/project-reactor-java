package academy.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

@Slf4j
public class FluxTest {

    @Test
    public void fluxSubscriber() {
        Flux<String> fluxString = Flux.just("Luiz", "Felipe", "Dojo", "Academy")
            .log();

        StepVerifier.create(fluxString)
            .expectNext("Luiz", "Felipe", "Dojo", "Academy")
            .verifyComplete();
    }

    @Test
    public void fluxSubscriberNumbers() {
        Flux<Integer> fluxInteger = Flux.range(1, 5)
            .log();

        fluxInteger.subscribe(i -> log.info("Number {}", i));

        log.info("----------------------------------------");
        StepVerifier.create(fluxInteger)
            .expectNext(1, 2, 3, 4, 5)
            .verifyComplete();
    }

    @Test
    public void fluxSubscriberFromList() {
        Flux<Integer> fluxInteger = Flux.fromIterable(List.of(1, 2, 3, 4, 5))
            .log();

        fluxInteger.subscribe(i -> log.info("Number {}", i));

        log.info("----------------------------------------");
        StepVerifier.create(fluxInteger)
            .expectNext(1, 2, 3, 4, 5)
            .verifyComplete();
    }

    @Test
    public void fluxSubscriberNumbersError() {
        Flux<Integer> fluxInteger = Flux.range(1, 5)
            .log()
            .map(i -> {
                if (i == 4) {
                    throw new IndexOutOfBoundsException("index error");
                }
                return i;
            });

        fluxInteger.subscribe(i -> log.info("Number {}", i), Throwable::printStackTrace,
            () -> log.info("DONE!"), subscription -> subscription.request(3));

        log.info("----------------------------------------");
        StepVerifier.create(fluxInteger)
            .expectNext(1, 2, 3)
            .expectError(IndexOutOfBoundsException.class)
            .verify();
    }

    @Test
    public void fluxSubscriberNumbersUglyBackpressure() {
        Flux<Integer> flux = Flux.range(1, 10)
            .log();

        flux.subscribe(new Subscriber<>() {
            private int count = 0;
            private Subscription subscription;
            private final int requestCount = 2;

            @Override
            public void onSubscribe(final Subscription subscription) {
                this.subscription = subscription;
                subscription.request(requestCount);
            }

            @Override
            public void onNext(final Integer integer) {
                count++;
                if (count >= requestCount) {
                    count = 0;
                    subscription.request(requestCount);
                }
            }

            @Override
            public void onError(final Throwable throwable) {

            }

            @Override
            public void onComplete() {

            }
        });

        log.info("----------------------------------------");
        StepVerifier.create(flux)
            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .verifyComplete();
    }

    @Test
    public void fluxSubscriberNumbersNotSoUglyBackpressure() {
        Flux<Integer> flux = Flux.range(1, 10)
            .log();

        flux.subscribe(new BaseSubscriber<>() {
            private int count = 0;
            private final int requestCount = 2;

            @Override
            protected void hookOnSubscribe(final Subscription subscription) {
                request(requestCount);
            }

            @Override
            protected void hookOnNext(final Integer value) {
                count++;
                if (count >= requestCount) {
                    count = 0;
                    request(requestCount);
                }
            }
        });

        log.info("----------------------------------------");
        StepVerifier.create(flux)
            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .verifyComplete();
    }

    @Test
    public void fluxSubscriberPrettyBackpressure() {
        Flux<Integer> fluxInteger = Flux.range(1, 10)
            .log()
            .limitRate(3);

        fluxInteger.subscribe(i -> log.info("Number {}", i));

        log.info("----------------------------------------");
        StepVerifier.create(fluxInteger)
            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .verifyComplete();
    }

    @Test
    public void fluxSubscriberIntervalOne() throws Exception {
        Flux<Long> interval = Flux.interval(Duration.ofMillis(100))
            .take(10)
            .log();

        interval.subscribe(i -> log.info("Number {}", i));

        Thread.sleep(3000);
    }

    @Test
    public void fluxSubscriberIntervalTwo() throws Exception {
        StepVerifier.withVirtualTime(this::createInterval)
            .expectSubscription()
            //            .expectNoEvent(Duration.ofHours(24))
            .expectNoEvent(Duration.ofDays(1))
            .thenAwait(Duration.ofDays(2))
            .expectNext(0L)
            .expectNext(1L)
            .thenCancel()
            .verify();
    }

    @Test
    public void connectableFlux() throws Exception {
        ConnectableFlux<Integer> connectableFlux = Flux.range(1, 10)
            .log()
            .delayElements(Duration.ofMillis(100))
            .publish();

        //        connectableFlux.connect();
        //
        //        log.info("Thread sleeping for 300ms");
        //        Thread.sleep(300);
        //
        //        connectableFlux.subscribe(i -> log.info("Sub1 number {}", i));
        //
        //        log.info("Thread sleeping for 200ms");
        //        Thread.sleep(200);
        //
        //        connectableFlux.subscribe(i -> log.info("Sub2 number {}", i));

        StepVerifier.create(connectableFlux)
            .then(connectableFlux::connect)
            .thenConsumeWhile(i -> i <= 5)
            .expectNext(6, 7, 8, 9, 10)
            .expectComplete()
            .verify();
    }

    @Test
    public void connectableFluxAutoConnect() throws Exception {
        Flux<Integer> fluxAutoConnect = Flux.range(1, 5)
            .log()
            .delayElements(Duration.ofMillis(100))
            .publish()
            .autoConnect(2);

        StepVerifier
            .create(fluxAutoConnect)
            .then(fluxAutoConnect::subscribe)
            .expectNext(1, 2, 3, 4, 5)
            .expectComplete()
            .verify();
    }

    private Flux<Long> createInterval() {
        return Flux.interval(Duration.ofDays(1))
            .log();
    }

}
