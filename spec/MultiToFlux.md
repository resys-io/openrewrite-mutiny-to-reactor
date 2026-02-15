| Mutiny (`Multi`)                                         | Semantics                                    | Reactor (`Flux`) Equivalent           |
|----------------------------------------------------------|----------------------------------------------|---------------------------------------|
| `Multi.createFrom().items(1, 2, 3)`                      | Emits the given items and completes          | `Flux.just(1, 2, 3)`                  |
| `Multi.createFrom().iterable(list)`                      | Emits items from an `Iterable` and completes | `Flux.fromIterable(list)`             |
| `Multi.createFrom().items(stream)`                       | Emits items from a `Stream` and completes    | `Flux.fromStream(stream)`             |
| `Multi.createFrom().range(start, count)`                 | Emits a range of integers                    | `Flux.range(start, count)`            |
| `Multi.createFrom().failure(e)`                          | Emits an error                               | `Flux.error(e)`                       |
| `Multi.createFrom().empty()`                             | Completes without emitting                   | `Flux.empty()`                        |
| `Multi.createFrom().nothing()`                           | Never emits and never completes (no signals) | `Flux.never()`                        |
| `Multi.createFrom().publisher(pub)`                      | Wraps an existing `Publisher`                | `Flux.from(pub)`                      |
| `Multi.createFrom().ticks().every(Duration.ofMillis(x))` | Emits a tick every `x` milliseconds          | `Flux.interval(Duration.ofMillis(x))` |
| `Multi.createFrom().emitter(emitter -> {…})`             | Programmatic emitter                         | `Flux.create(sink -> {…})`            |

✅ Special notes
Failure: Multi.failure(e) → Flux.error(e) (1:1 mapping).
Empty: Multi.empty() → Flux.empty() (no items, immediate completion).
Nothing: Multi.nothing() → Flux.never() (never emits, never completes).
Emitter: Both Mutiny and Reactor provide programmatic create(...) APIs that let you push items manually.
Backpressure: Both Flux and Multi are Reactive Streams compliant, so backpressure is preserved across conversions via .toPublisher().