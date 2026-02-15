| Mutiny (`Multi`) Operator                                     | Semantics (Mutiny)                                              | Reactor (`Flux`) Equivalent                                                                 |
|---------------------------------------------------------------|-----------------------------------------------------------------|---------------------------------------------------------------------------------------------|
| `multi.onItem().transform(x -> f(x))`                         | Transform each item synchronously                               | `flux.map(x -> f(x))`                                                                       |
| `multi.onItem().transformToUni(x -> Uni.createFrom()...)`     | For each item, async transform to a `Uni`, then flatten         | `flux.flatMap(x -> Mono.from(...))`                                                         |
| `multi.onItem().transformToMulti(x -> Multi.createFrom()...)` | For each item, async transform to another `Multi`, then flatten | `flux.flatMap(x -> Flux.from(...))`                                                         |
| `multi.onItem().ignore().andContinueWithNull()`               | Drop items, emit `null` (⚠ Reactor disallows `null`)            | Not possible directly; best: `flux.ignoreElements().then(Mono.empty())`                     |
| `multi.onItem().ignore().andContinueWith(item)`               | Drop original, continue with fixed item                         | `flux.ignoreElements().thenReturn(item)`                                                    |
| `multi.onItem().invoke(x -> sideEffect(x))`                   | Execute side-effect for each item, item unchanged               | `flux.doOnNext(x -> sideEffect(x))`                                                         |
| `multi.onItem().delayIt().by(Duration.ofMillis(x))`           | Delay each item by duration                                     | `flux.delayElements(Duration.ofMillis(x))`                                                  |
| `multi.onItem().ifNull().continueWith(y)`                     | Replace `null` item with another item                           | Reactor disallows `null`; use `flux.map(x -> x == null ? y : x)`                            |
| `multi.onItem().ifNull().failWith(e)`                         | If item is `null`, fail with error                              | Reactor disallows `null`; use `flux.flatMap(x -> x == null ? Flux.error(e) : Flux.just(x))` |

Null handling: Mutiny allows null items in Multi, Reactor strictly disallows null in Flux. When converting, you must map null → empty, replacement, or error explicitly.

Async transforms:

transformToUni → flatMap with Mono.

transformToMulti → flatMap with Flux.

Side effects: invoke ↔ doOnNext.

Delays: Mutiny’s delayIt().by() ↔ Reactor’s delayElements.