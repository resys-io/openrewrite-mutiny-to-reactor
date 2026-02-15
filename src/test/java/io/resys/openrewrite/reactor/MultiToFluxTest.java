package io.resys.openrewrite.reactor;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class MultiToFluxTest implements RewriteTest {
  @Override
  public void defaults(RecipeSpec spec) {
    spec.recipe(new MultiToFlux())
      .typeValidationOptions(org.openrewrite.test.TypeValidation.none());
  }

  @Test
  void transformsMultiCreateFromItemsToFluxJust() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<Integer> test() {
              return Multi.createFrom().items(1, 2, 3);
            }
          }
          """,
        """
          package com.yourorg;
          
          import reactor.core.publisher.Flux;

          class FooBar {
            Flux<Integer> test() {
              return Flux.just(1, 2, 3);
            }
          }
          """
      )
    );
  }

  @Test
  void transformsMultiCreateFromSingleItemToFluxJust() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().items("test");
            }
          }
          """,
        """
          package com.yourorg;
          
          import reactor.core.publisher.Flux;

          class FooBar {
            Flux<String> test() {
              return Flux.just("test");
            }
          }
          """
      )
    );
  }

  @Test
  void transformsMultiCreateFromIterableToFluxFromIterable() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;
          import java.util.List;

          class FooBar {
            Multi<String> test(List<String> items) {
              return Multi.createFrom().iterable(items);
            }
          }
          """,
        """
          package com.yourorg;
          
          import reactor.core.publisher.Flux;
          
          import java.util.List;

          class FooBar {
            Flux<String> test(List<String> items) {
              return Flux.fromIterable(items);
            }
          }
          """
      )
    );
  }

  @Test
  void transformsMultiCreateFromStreamToFluxFromStream() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;
          
          import java.util.stream.Stream;

          class FooBar {
            Multi<String> test(Stream<String> stream) {
              return Multi.createFrom().items(stream);
            }
          }
          """,
        """
          package com.yourorg;
          
          import reactor.core.publisher.Flux;
          
          import java.util.stream.Stream;

          class FooBar {
            Flux<String> test(Stream<String> stream) {
              return Flux.fromStream(stream);
            }
          }
          """
      )
    );
  }

  @Test
  void transformsMultiCreateFromRangeToFluxRange() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<Integer> test() {
              return Multi.createFrom().range(1, 10);
            }
          }
          """,
        """
          package com.yourorg;
          
          import reactor.core.publisher.Flux;

          class FooBar {
            Flux<Integer> test() {
              return Flux.range(1, 10);
            }
          }
          """
      )
    );
  }

  @Test
  void transformsMultiCreateFromFailureToFluxError() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().failure(new RuntimeException("error"));
            }
          }
          """,
        """
          package com.yourorg;
          
          import reactor.core.publisher.Flux;

          class FooBar {
            Flux<String> test() {
              return Flux.error(new RuntimeException("error"));
            }
          }
          """
      )
    );
  }

  @Test
  void transformsMultiCreateFromEmptyToFluxEmpty() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().empty();
            }
          }
          """,
        """
          package com.yourorg;
          
          import reactor.core.publisher.Flux;

          class FooBar {
            Flux<String> test() {
              return Flux.empty();
            }
          }
          """
      )
    );
  }

  @Test
  void transformsMultiCreateFromNothingToFluxNever() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().nothing();
            }
          }
          """,
        """
          package com.yourorg;
          
          import reactor.core.publisher.Flux;

          class FooBar {
            Flux<String> test() {
              return Flux.never();
            }
          }
          """
      )
    );
  }

  @Test
  void transformsMultiCreateFromPublisherToFluxFrom() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;
          import org.reactivestreams.Publisher;

          class FooBar {
            Multi<String> test(Publisher<String> publisher) {
              return Multi.createFrom().publisher(publisher);
            }
          }
          """,
        """
          package com.yourorg;
          
          import org.reactivestreams.Publisher;
          import reactor.core.publisher.Flux;

          class FooBar {
            Flux<String> test(Publisher<String> publisher) {
              return Flux.from(publisher);
            }
          }
          """
      )
    );
  }

  @Test
  void transformsMultiCreateFromTicksEveryToFluxInterval() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;
          
          import java.time.Duration;

          class FooBar {
            Multi<Long> test() {
              return Multi.createFrom().ticks().every(Duration.ofMillis(100));
            }
          }
          """,
        """
          package com.yourorg;
          
          import reactor.core.publisher.Flux;
          
          import java.time.Duration;

          class FooBar {
            Flux<Long> test() {
              return Flux.interval(Duration.ofMillis(100));
            }
          }
          """
      )
    );
  }

  @Test
  void transformsMultiCreateFromEmitterToFluxCreate() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().emitter(emitter -> emitter.emit("test"));
            }
          }
          """,
        """
          package com.yourorg;
          
          import reactor.core.publisher.Flux;

          class FooBar {
            Flux<String> test() {
              return Flux.create(emitter -> emitter.emit("test"));
            }
          }
          """
      )
    );
  }

  @Test
  void doesNotChangeOtherMethodCalls() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          class FooBar {
              public String hello() { return ""; }
          }
          """
      )
    );
  }

  @Test
  void transformsMultipleMultiCreateCalls() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;
          import java.util.List;

          class FooBar {
              Multi<String> test1() {
                  return Multi.createFrom().items("a", "b", "c");
              }
              
              Multi<Integer> test2() {
                  return Multi.createFrom().range(1, 5);
              }
              
              Multi<String> test3() {
                  return Multi.createFrom().empty();
              }
              
              Multi<String> test4(List<String> items) {
                  return Multi.createFrom().iterable(items);
              }
          }
          """,
        """
          package com.yourorg;

          import reactor.core.publisher.Flux;
          
          import java.util.List;

          class FooBar {
              Flux<String> test1() {
                  return Flux.just("a", "b", "c");
              }
              
              Flux<Integer> test2() {
                  return Flux.range(1, 5);
              }
              
              Flux<String> test3() {
                  return Flux.empty();
              }
              
              Flux<String> test4(List<String> items) {
                  return Flux.fromIterable(items);
              }
          }
          """
      )
    );
  }
}