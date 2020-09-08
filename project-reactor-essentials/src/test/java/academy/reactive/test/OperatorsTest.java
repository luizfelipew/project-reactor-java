package academy.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class OperatorsTest {

    @Test
    public void subscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
            .map(i -> {
                log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                return i;
            })
            //            .subscribeOn(Schedulers.single())
            .subscribeOn(Schedulers.boundedElastic())
            .map(i -> {
                log.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                return i;
            });

        StepVerifier.create(flux)
            .expectSubscription()
            .expectNext(1, 2, 3, 4)
            .verifyComplete();
    }

    @Test
    public void publishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
            .map(i -> {
                log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                return i;
            })
            .publishOn(Schedulers.boundedElastic())
            .map(i -> {
                log.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                return i;
            });

        flux.subscribe();

        StepVerifier.create(flux)
            .expectSubscription()
            .expectNext(1, 2, 3, 4)
            .verifyComplete();
    }

    @Test
    public void multipleSubscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
            .subscribeOn(Schedulers.boundedElastic())
            .map(i -> {
                log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                return i;
            })
            .subscribeOn(Schedulers.single())
            .map(i -> {
                log.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                return i;
            });

        StepVerifier.create(flux)
            .expectSubscription()
            .expectNext(1, 2, 3, 4)
            .verifyComplete();
    }

    @Test
    public void multiplePublishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
            .publishOn(Schedulers.single())
            .map(i -> {
                log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                return i;
            })
            .publishOn(Schedulers.boundedElastic())
            .map(i -> {
                log.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                return i;
            });

        flux.subscribe();

        StepVerifier.create(flux)
            .expectSubscription()
            .expectNext(1, 2, 3, 4)
            .verifyComplete();
    }

    @Test
    public void publishAndSubscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
            .publishOn(Schedulers.single())
            .map(i -> {
                log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                return i;
            })
            .subscribeOn(Schedulers.boundedElastic())
            .map(i -> {
                log.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                return i;
            });

        flux.subscribe();

        StepVerifier.create(flux)
            .expectSubscription()
            .expectNext(1, 2, 3, 4)
            .verifyComplete();
    }

    @Test
    public void subscribeAndPublishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
            .subscribeOn(Schedulers.single())
            .map(i -> {
                log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                return i;
            })
            .publishOn(Schedulers.boundedElastic())
            .map(i -> {
                log.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                return i;
            });

        flux.subscribe();

        StepVerifier.create(flux)
            .expectSubscription()
            .expectNext(1, 2, 3, 4)
            .verifyComplete();
    }


    @Test
    public void subscribeOnIO() throws Exception{
        Mono<List<String>> list = Mono.fromCallable(() -> Files.readAllLines(Path.of("text-file")))
            .log()
            .subscribeOn(Schedulers.boundedElastic());
        // executing thread in background (boundedElastic)
        // boundedElastic is recommended in the documentation

//        list.subscribe(s -> log.info("{}", s));

//        Thread.sleep(2000);

        StepVerifier.create(list)
            .expectSubscription()
            .thenConsumeWhile(l -> {
                Assertions.assertFalse(l.isEmpty());
                log.info("Size {}", l.size());
                return true;
            })
            .verifyComplete();
    }

    @Test
    public void switchIfEmptyOperator() {
        Flux<Object> flux = emptyFlux()
            .switchIfEmpty(Flux.just("not empty anymore"))
            .log();

        StepVerifier.create(flux)
            .expectSubscription()
            .expectNext("not empty anymore")
            .expectComplete()
            .verify();
    }

    @Test
    public void deferOperation() throws Exception {
        Mono<Long> just = Mono.just(System.currentTimeMillis());
        Mono<Long> defer = Mono.defer(() -> Mono.just(System.currentTimeMillis()));

//        just.subscribe(i -> log.info("time {}", i));
//        Thread.sleep(100);
//        just.subscribe(i -> log.info("time {}", i));
//        Thread.sleep(100);
//        just.subscribe(i -> log.info("time {}", i));
//        Thread.sleep(100);
//        just.subscribe(i -> log.info("time {}", i));

        defer.subscribe(i -> log.info("time {}", i));
        Thread.sleep(100);
        defer.subscribe(i -> log.info("time {}", i));
        Thread.sleep(100);
        defer.subscribe(i -> log.info("time {}", i));
        Thread.sleep(100);
        defer.subscribe(i -> log.info("time {}", i));

        AtomicLong atomicLong = new AtomicLong();
        defer.subscribe(atomicLong::set);
        Assertions.assertTrue(atomicLong.get() > 0);


    }

    private Flux<Object> emptyFlux() {
        return Flux.empty();
    }

}
