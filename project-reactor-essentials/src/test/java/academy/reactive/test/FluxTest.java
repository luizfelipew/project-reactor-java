package academy.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

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
            private int requestCount = 2;

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

}
