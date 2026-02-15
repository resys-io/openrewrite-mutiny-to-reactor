# SmallRye Mutiny to Project Reactor Migration - Implementation Summary

## Overview

This project provides a complete set of OpenRewrite recipes for migrating reactive code from SmallRye Mutiny to Project Reactor. The implementation handles all major reactive patterns including creation, transformation, error handling, and chaining.

## Test Results

✅ **All 84 tests passing**

```
Tests run: 84, Failures: 0, Errors: 0, Skipped: 0
Build: SUCCESS
```

## Implemented Recipes

### 1. UniToMono (10 tests)
Transforms `Uni.createFrom()` factory methods to `Mono` equivalents.

**Key Transformations:**
- `Uni.createFrom().item(x)` → `Mono.just(x)`
- `Uni.createFrom().nullItem()` → `Mono.empty()`
- `Uni.createFrom().voidItem()` → `Mono.empty()`
- `Uni.createFrom().failure(e)` → `Mono.error(e)`
- `Uni.createFrom().nothing()` → `Mono.never()`
- `Uni.createFrom().optional(opt)` → `Mono.justOrEmpty(opt)`
- `Uni.createFrom().emitter(e -> ...)` → `Mono.create(e -> ...)`
- `Uni.createFrom().publisher(p)` → `Mono.from(p)`

**Specification:** `spec/UniToMono.md`

---

### 2. UniOnItemToMono (12 tests) ✨ NEW
Transforms `Uni.onItem()` operators to `Mono` equivalents.

**Key Transformations:**
- `uni.onItem().transform(f)` → `mono.map(f)`
- `uni.onItem().transformToUni(f)` → `mono.flatMap(f)`
- `uni.onItem().transformToMulti(f)` → `mono.flatMapMany(f)`
- `uni.onItem().invoke(c)` → `mono.doOnNext(c)`
- `uni.onItem().delayIt().by(d)` → `mono.delayElement(d)`
- `uni.onItem().ignore().andContinueWithNull()` → `mono.then(Mono.empty())`
- `uni.onItem().ignore().andContinueWith(x)` → `mono.thenReturn(x)`
- `uni.onItem().ifNull().continueWith(x)` → `mono.switchIfEmpty(Mono.just(x))`
- `uni.onItem().ifNull().failWith(e)` → `mono.switchIfEmpty(Mono.error(e))`

**Specification:** `spec/UniOnItemToMono.md`

---

### 3. UniOnFailureToMono (11 tests)
Transforms `Uni.onFailure()` error handling to `Mono` equivalents.

**Key Transformations:**
- `uni.onFailure().recoverWithItem(x)` → `mono.onErrorReturn(x)`
- `uni.onFailure().recoverWithItem(f)` → `mono.onErrorResume(e -> Mono.just(f(e)))`
- `uni.onFailure().recoverWithNull()` → `mono.onErrorResume(e -> Mono.empty())`
- `uni.onFailure().recoverWithUni(u)` → `mono.onErrorResume(e -> u)`
- `uni.onFailure().invoke(c)` → `mono.doOnError(c)`
- `uni.onFailure().retry().atMost(n)` → `mono.retry(n)`
- `uni.onFailure().retry().indefinitely()` → `mono.retry()`
- `uni.onFailure().retry().withBackOff(min, max)` → `mono.retryWhen(Retry.backoff(...))`

**Specification:** `spec/UniOnFailureToMono.md`

---

### 4. MultiToFlux (13 tests)
Transforms `Multi.createFrom()` factory methods to `Flux` equivalents.

**Key Transformations:**
- `Multi.createFrom().items(x, y, z)` → `Flux.just(x, y, z)`
- `Multi.createFrom().iterable(it)` → `Flux.fromIterable(it)`
- `Multi.createFrom().range(start, count)` → `Flux.range(start, count)`
- `Multi.createFrom().failure(e)` → `Flux.error(e)`
- `Multi.createFrom().empty()` → `Flux.empty()`
- `Multi.createFrom().publisher(p)` → `Flux.from(p)`
- `Multi.createFrom().ticks().every(d)` → `Flux.interval(d)`

**Specification:** `spec/MultiToFlux.md`

---

### 5. MultiOnItemToFlux (9 tests) ✨ NEW
Transforms `Multi.onItem()` operators to `Flux` equivalents.

**Key Transformations:**
- `multi.onItem().transform(f)` → `flux.map(f)`
- `multi.onItem().transformToUni(f)` → `flux.flatMap(f)` (with Mono)
- `multi.onItem().transformToMulti(f)` → `flux.flatMap(f)` (with Flux)
- `multi.onItem().invoke(c)` → `flux.doOnNext(c)`
- `multi.onItem().delayIt().by(d)` → `flux.delayElements(d)`
- `multi.onItem().ignore().andContinueWith(x)` → `flux.ignoreElements().thenReturn(x)`

**Note:** `andContinueWithNull()` is not directly supported as Reactor forbids null values.

**Specification:** `spec/MultiOnItemToFlux.md`

---

### 6. MultiOnFailureToFlux (12 tests) ✨ NEW
Transforms `Multi.onFailure()` error handling to `Flux` equivalents.

**Key Transformations:**
- `multi.onFailure().recoverWithItem(x)` → `flux.onErrorReturn(x)`
- `multi.onFailure().recoverWithItem(f)` → `flux.onErrorResume(e -> Flux.just(f(e)))`
- `multi.onFailure().recoverWithNull()` → `flux.onErrorResume(e -> Flux.empty())`
- `multi.onFailure().recoverWithMulti(m)` → `flux.onErrorResume(e -> m)`
- `multi.onFailure().invoke(c)` → `flux.doOnError(c)`
- `multi.onFailure().retry().atMost(n)` → `flux.retry(n)`
- `multi.onFailure().retry().indefinitely()` → `flux.retry()`
- `multi.onFailure().retry().withBackOff(min, max)` → `flux.retryWhen(Retry.backoff(...))`

**Specification:** `spec/MultiOnFailureToFlux.md`

---

### 7. ChainedTransformationsTest (7 tests)
Validates that chained method calls are transformed correctly:

```java
// Example
Uni.createFrom().item("test")
  .onFailure().retry().atMost(3)
  .onFailure().invoke(e -> log(e))
  .onFailure().recoverWithItem("fallback")

// Transforms to:
Mono.just("test")
  .retry(3)
  .doOnError(e -> log(e))
  .onErrorReturn("fallback")
```

**See:** `CHAINED_CALLS.md` for detailed documentation

---

### 8. CombinedRecipesTest (10 tests)
Validates that multiple recipes work together in a complete migration scenario:

```java
// Example: Full migration
Uni.createFrom().item("test")
  .onItem().transform(s -> s.toUpperCase())
  .onFailure().retry().atMost(3)

// Transforms to:
Mono.just("test")
  .map(s -> s.toUpperCase())
  .retry(3)
```

With type changes: `Uni<T>` → `Mono<T>`, `Multi<T>` → `Flux<T>`

---

## Chained Method Call Handling

All recipes correctly handle chained method invocations through:

1. **AST Visitor Pattern** - Processes method invocations independently
2. **Bottom-Up Processing** - Innermost transformations happen first
3. **Chain Preservation** - Only transforms relevant parts, preserves the rest
4. **Recipe Composition** - Multiple recipes can be applied sequentially

See `CHAINED_CALLS.md` for detailed technical explanation.

---

## Special Handling

### Lambda Functions
The recipes intelligently detect and handle:
- **Inline lambdas**: `e -> "error: " + e.getMessage()`
- **Method references**: `this::handleError`
- **Function variables**: Passed as parameters

### Null Handling
Reactor forbids null values, so:
- `nullItem()` → `Mono.empty()` / `Flux.empty()`
- `recoverWithNull()` → `onErrorResume(e -> Mono.empty())`
- Null-related operators map to empty completions

---

## Project Statistics

| Metric | Count |
|--------|-------|
| **Recipe Classes** | 6 |
| **Test Classes** | 8 |
| **Total Tests** | 84 |
| **Specification Files** | 6 |
| **Test Success Rate** | 100% |
| **Lines of Code (recipes)** | ~1,200 |
| **Lines of Code (tests)** | ~1,800 |

---

## File Structure

```
src/main/java/io/resys/rewrite/
├── UniToMono.java
├── UniOnItemToMono.java            ← NEW
├── UniOnFailureToMono.java
├── MultiToFlux.java
├── MultiOnItemToFlux.java          ← NEW
└── MultiOnFailureToFlux.java       ← NEW

src/test/java/io/resys/rewrite/
├── UniToMonoTest.java
├── UniOnItemToMonoTest.java        ← NEW
├── UniOnFailureToMonoTest.java
├── MultiToFluxTest.java
├── MultiOnItemToFluxTest.java      ← NEW
├── MultiOnFailureToFluxTest.java   ← NEW
├── ChainedTransformationsTest.java
└── CombinedRecipesTest.java

spec/
├── UniToMono.md
├── UniOnItemToMono.md
├── UniOnFailureToMono.md
├── MultiToFlux.md
├── MultiOnItemToFlux.md
└── MultiOnFailureToFlux.md
```

---

## Usage Example

### Applying All Recipes Together

To perform a complete migration from Mutiny to Reactor, apply the recipes in this order:

1. `UniToMono` - Transform Uni creation
2. `UniOnItemToMono` - Transform Uni item operators
3. `UniOnFailureToMono` - Transform Uni error handling
4. `MultiToFlux` - Transform Multi creation
5. `MultiOnItemToFlux` - Transform Multi item operators
6. `MultiOnFailureToFlux` - Transform Multi error handling

This ensures all patterns are covered and chained operations are transformed correctly.

### Example Input

```java
public class UserService {
  public Uni<User> getUser(String id) {
    return Uni.createFrom().item(findUserById(id))
      .onItem().transform(user -> enrichUser(user))
      .onItem().invoke(user -> log.info("Retrieved user: {}", user))
      .onFailure().retry().atMost(3)
      .onFailure().invoke(e -> log.error("Failed to get user", e))
      .onFailure().recoverWithItem(User.ANONYMOUS);
  }
}
```

### Example Output

```java
public class UserService {
  public Mono<User> getUser(String id) {
    return Mono.just(findUserById(id))
      .map(user -> enrichUser(user))
      .doOnNext(user -> log.info("Retrieved user: {}", user))
      .retry(3)
      .doOnError(e -> log.error("Failed to get user", e))
      .onErrorReturn(User.ANONYMOUS);
  }
}
```

---

## Known Limitations

1. **Null Values**: Reactor strictly forbids null, while Mutiny allows it. Null-emitting operators are transformed to empty completions.

2. **Multi.onItem().andContinueWithNull()**: Not directly supported in Reactor; left as-is with a comment in the spec.

3. **Complex Lambda Bodies**: Lambdas with block bodies (not just expressions) are handled but may need manual review.

4. **Type Imports**: Some transformations use fully qualified class names to avoid import management issues.

---

## Build and Test

```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=UniOnItemToMonoTest

# Clean and rebuild
mvn clean compile test
```

---

## Next Steps

To use these recipes in your project:

1. Build the recipe JAR: `mvn package`
2. Add the JAR to your project's dependencies
3. Configure OpenRewrite in your `pom.xml` or `build.gradle`
4. Run the migration: `mvn rewrite:run`
5. Review and test the migrated code

For more details on OpenRewrite configuration, see the [OpenRewrite documentation](https://docs.openrewrite.org/).
