| Mutiny (`Uni`)                                | Semantics                                          | Reactor (`Mono`) Equivalent                                                         |
|-----------------------------------------------|----------------------------------------------------|-------------------------------------------------------------------------------------|
| `Uni.createFrom().item(x)`                    | Emits a single item `x` and completes              | `Mono.just(x)`                                                                      |
| `Uni.createFrom().nullItem()`                 | Emits `null` and completes                         | `Mono.empty()` (since `Mono.just(null)` is invalid, Reactor treats `null` as empty) |
| `Uni.createFrom().voidItem()`                 | Emits a `Void` (no value) and completes            | `Mono.empty()`                                                                      |
| `Uni.createFrom().failure(e)`                 | Emits an error                                     | `Mono.error(e)`                                                                     |
| `Uni.createFrom().nothing()`                  | Never emits and never completes (no signal at all) | `Mono.never()`                                                                      |
| `Uni.createFrom().optional(Optional.of(x))`   | Emits `x` if present, otherwise completes empty    | `Mono.justOrEmpty(Optional.of(x))`                                                  |
| `Uni.createFrom().optional(Optional.empty())` | Completes empty                                    | `Mono.empty()`                                                                      |
| `Uni.createFrom().emitter(emitter -> {…})`    | Programmatic emitter, may emit item or failure     | `Mono.create(sink -> {…})`                                                          |
| `Uni.createFrom().publisher(publisher)`       | Wraps a `Publisher` that emits at most one item    | `Mono.from(publisher)`                                                              |
