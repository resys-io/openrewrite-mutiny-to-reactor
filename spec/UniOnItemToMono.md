| Mutiny (`Uni`) Operator                                     | Semantics (Mutiny)                                           | Reactor (`Mono`) Equivalent                                                   |
|-------------------------------------------------------------|--------------------------------------------------------------|-------------------------------------------------------------------------------|
| `uni.onItem().transform(x -> f(x))`                         | Transform the item synchronously                             | `mono.map(x -> f(x))`                                                         |
| `uni.onItem().transformToUni(x -> Uni.createFrom()...)`     | Flat-map to another `Uni` (async transform)                  | `mono.flatMap(x -> Mono.from(...))`                                           |
| `uni.onItem().transformToMulti(x -> Multi.createFrom()...)` | Flat-map to a `Multi`                                        | `mono.flatMapMany(x -> Flux.from(...))`                                       |
| `uni.onItem().ignore().andContinueWithNull()`               | Drop the item and complete with `null`                       | `mono.then(Mono.empty())` or `mono.thenReturn(null)` (be careful with `null`) |
| `uni.onItem().ignore().andContinueWith(item)`               | Drop original, continue with fixed item                      | `mono.thenReturn(item)`                                                       |
| `uni.onItem().invoke(x -> sideEffect(x))`                   | Execute a side-effect when item arrives, doesn’t change item | `mono.doOnNext(x -> sideEffect(x))`                                           |
| `uni.onItem().delayIt().by(Duration.ofMillis(x))`           | Delay emission of the item by duration                       | `mono.delayElement(Duration.ofMillis(x))`                                     |
| `uni.onItem().ifNull().continueWith(item)`                  | If the item is `null`, replace with another item             | `mono.switchIfEmpty(Mono.just(item))`                                         |
| `uni.onItem().ifNull().failWith(e)`                         | If item is `null`, fail with exception                       | `mono.switchIfEmpty(Mono.error(e))`                                           |

Both Uni and Mono treat null specially, but Reactor does not allow null items. Mutiny allows null, so when bridging you typically translate null to empty completion (Mono.empty()) or to an explicit replacement (Mono.justOrEmpty(...)).
For async transformations, Mutiny’s transformToUni ↔ Reactor’s flatMap.
For side-effects, both libraries provide invoke ↔ doOnNext.
For delays, both provide matching operators.