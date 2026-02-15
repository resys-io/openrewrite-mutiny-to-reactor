package io.resys.openrewrite.reactor;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class UniToMonoTest implements RewriteTest {
  @Override
  public void defaults(RecipeSpec spec) {
    spec.recipe(new UniToMono())
      .typeValidationOptions(org.openrewrite.test.TypeValidation.none());
  }

  @Test
  void transformsUniCreateFromItemToMonoJust() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
          
            Uni<String> test() {
              return Uni.createFrom().item("test");
            }
          
          }
          """,
        """
          package com.yourorg;
          
          import reactor.core.publisher.Mono;

          class FooBar {
          
            Mono<String> test() {
              return Mono.just("test");
            }
          
          }
          """
      )
    );
  }

  @Test
  void transformsUniCreateVoidItemToMonoJust() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
          
            Uni<Void> test() {
              return Uni.createFrom().voidItem();
            }
          
          }
          """,
        """
          package com.yourorg;
          
          import reactor.core.publisher.Mono;

          class FooBar {
          
            Mono<Void> test() {
              return Mono.empty();
            }
          
          }
          """
      )
    );
  }

  @Test
  void transformsUniCreateNullItemToMonoJust() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
          
            Uni<String> test() {
              return Uni.createFrom().nullItem();
            }
          
          }
          """,
        """
          package com.yourorg;
          
          import reactor.core.publisher.Mono;

          class FooBar {
          
            Mono<String> test() {
              return Mono.empty();
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
  void transformsUniCreateFromFailureToMonoError() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().failure(new RuntimeException("error"));
            }
          }
          """,
        """
          package com.yourorg;
          
          import reactor.core.publisher.Mono;

          class FooBar {
            Mono<String> test() {
              return Mono.error(new RuntimeException("error"));
            }
          }
          """
      )
    );
  }

  @Test
  void transformsUniCreateFromNothingToMonoNever() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().nothing();
            }
          }
          """,
        """
          package com.yourorg;
          
          import reactor.core.publisher.Mono;

          class FooBar {
            Mono<String> test() {
              return Mono.never();
            }
          }
          """
      )
    );
  }

  @Test
  void transformsUniCreateFromOptionalToMonoJustOrEmpty() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;
          import java.util.Optional;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().optional(Optional.of("test"));
            }
          }
          """,
        """
          package com.yourorg;
          
          import reactor.core.publisher.Mono;
          
          import java.util.Optional;

          class FooBar {
            Mono<String> test() {
              return Mono.justOrEmpty(Optional.of("test"));
            }
          }
          """
      )
    );
  }

  @Test
  void transformsUniCreateFromEmitterToMonoCreate() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().emitter(emitter -> emitter.complete("test"));
            }
          }
          """,
        """
          package com.yourorg;
          
          import reactor.core.publisher.Mono;

          class FooBar {
            Mono<String> test() {
              return Mono.create(emitter -> emitter.complete("test"));
            }
          }
          """
      )
    );
  }

  @Test
  void transformsUniCreateFromPublisherToMonoFrom() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;
          import org.reactivestreams.Publisher;

          class FooBar {
            Uni<String> test(Publisher<String> publisher) {
              return Uni.createFrom().publisher(publisher);
            }
          }
          """,
        """
          package com.yourorg;
          
          import org.reactivestreams.Publisher;
          import reactor.core.publisher.Mono;

          class FooBar {
            Mono<String> test(Publisher<String> publisher) {
              return Mono.from(publisher);
            }
          }
          """
      )
    );
  }

  @Test
  void transformsMultipleUniCreateCalls() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
              Uni<String> test1() {
                  return Uni.createFrom().item("test1");
              }
              
              Uni<Integer> test2() {
                  return Uni.createFrom().item(42);
              }
              
              Uni<String> test3() {
                  return Uni.createFrom().nullItem();
              }
              
              Uni<Void> test4() {
                  return Uni.createFrom().voidItem();
              }
          }
          """,
        """
          package com.yourorg;

          import reactor.core.publisher.Mono;

          class FooBar {
              Mono<String> test1() {
                  return Mono.just("test1");
              }
              
              Mono<Integer> test2() {
                  return Mono.just(42);
              }
              
              Mono<String> test3() {
                  return Mono.empty();
              }
              
              Mono<Void> test4() {
                  return Mono.empty();
              }
          }
          """
      )
    );
  }
}