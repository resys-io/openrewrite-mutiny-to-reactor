| Mutiny (`Uni`) Operator                         | Semantics (Mutiny)                              | Reactor (`Mono`) Equivalent                      |
|-------------------------------------------------|-------------------------------------------------|--------------------------------------------------|
| `uni.onFailure().recoverWithItem(x)`            | On error, recover by emitting fallback item `x` | `mono.onErrorReturn(x)`                          |
| `uni.onFailure().recoverWithItem(f)`            | On error, call function → fallback item         | `mono.onErrorResume(e -> Mono.just(f.apply(e)))` |
| `uni.onFailure().recoverWithNull()`             | On error, recover by completing empty           | `mono.onErrorResume(e -> Mono.empty())`          |
| `uni.onFailure().recoverWithUni(u)`             | On error, recover with another `Uni`            | `mono.onErrorResume(e -> otherMono)`             |
| `uni.onFailure().invoke(e -> sideEffect(e))`    | Side-effect on error, item unchanged            | `mono.doOnError(e -> sideEffect(e))`             |
| `uni.onFailure().retry().atMost(n)`             | Retry at most `n` times                         | `mono.retry(n)`                                  |
| `uni.onFailure().retry().indefinitely()`        | Retry indefinitely                              | `mono.retry()` (with no args)                    |
| `uni.onFailure().retry().withBackOff(min, max)` | Retry with exponential backoff                  | `mono.retryWhen(Retry.backoff(...))`             |

Recovery: Mutiny’s recoverWithItem/recoverWithUni map to Reactor’s onErrorReturn/onErrorResume.

Side-effects: invoke ↔ doOnError.

Retries: both libraries support retry(n), retry() (infinite), and retryWhen for backoff strategies.

Null differences: Reactor forbids null, so any Mutiny operator that emits null must be mapped to empty or a placeholder value in Reactor.