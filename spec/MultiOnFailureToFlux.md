| Mutiny (`Multi`) Operator                         | Semantics (Mutiny)                                            | Reactor (`Flux`) Equivalent                                         |
|---------------------------------------------------|---------------------------------------------------------------|---------------------------------------------------------------------|
| `multi.onFailure().recoverWithItem(x)`            | On error, emit fallback item then complete                    | `flux.onErrorReturn(x)`                                             |
| `multi.onFailure().recoverWithItem(f)`            | On error, call function → fallback item                       | `flux.onErrorResume(e -> Flux.just(f.apply(e)))`                    |
| `multi.onFailure().recoverWithNull()`             | On error, emit `null` and complete (⚠ Reactor forbids `null`) | Not directly possible → use `flux.onErrorResume(e -> Flux.empty())` |
| `multi.onFailure().recoverWithMulti(m)`           | On error, recover with another `Multi`                        | `flux.onErrorResume(e -> otherFlux)`                                |
| `multi.onFailure().invoke(e -> sideEffect(e))`    | Side-effect on error, stream continues to fail                | `flux.doOnError(e -> sideEffect(e))`                                |
| `multi.onFailure().retry().atMost(n)`             | Retry at most `n` times                                       | `flux.retry(n)`                                                     |
| `multi.onFailure().retry().indefinitely()`        | Retry indefinitely                                            | `flux.retry()`                                                      |
| `multi.onFailure().retry().withBackOff(min, max)` | Retry with exponential backoff                                | `flux.retryWhen(Retry.backoff(...))`                                |

Recovery: Mutiny’s recoverWithItem/recoverWithUni map to Reactor’s onErrorReturn/onErrorResume.

Side-effects: invoke ↔ doOnError.

Retries: both libraries support retry(n), retry() (infinite), and retryWhen for backoff strategies.

Null differences: Reactor forbids null, so any Mutiny operator that emits null must be mapped to empty or a placeholder value in Reactor.