# Chained Method Call Handling

## Overview

Reactive APIs commonly use method chaining to build reactive pipelines. The rewrite recipes in this project are specifically designed to handle these chained calls correctly.

## Example Chained Call

```java
// Input (Mutiny)
Uni.createFrom().item("test")
  .onFailure().retry().atMost(3)
  .onFailure().invoke(e -> System.err.println(e))
  .onFailure().recoverWithItem("fallback");

// Output (Reactor)
Mono.just("test")
  .retry(3)
  .doOnError(e -> System.err.println(e))
  .onErrorReturn("fallback");
```

## How It Works

### AST Visitor Pattern

The recipes use OpenRewrite's `JavaIsoVisitor` which traverses the Abstract Syntax Tree (AST) of the Java code. The visitor pattern processes method invocations and transforms them appropriately.

### Method Invocation Structure

In the AST, chained method calls are represented as nested `J.MethodInvocation` nodes:

```
MethodInvocation: recoverWithItem("fallback")
  └─ Select: MethodInvocation: onFailure()
       └─ Select: MethodInvocation: item("test")
            └─ Select: MethodInvocation: createFrom()
                 └─ Select: Identifier: Uni
```

### Transformation Strategy

1. **Bottom-Up Processing**: The visitor processes the AST from the innermost nodes outward, so each method invocation is handled independently.

2. **Selective Matching**: Each recipe only transforms the specific method patterns it's designed for:
   - `UniToMono`: Transforms `Uni.createFrom().item()` → `Mono.just()`
   - `UniOnFailureToMono`: Transforms `.onFailure().recoverWithItem()` → `.onErrorReturn()`

3. **Preserving Chains**: When transforming a method invocation, the recipe:
   - Extracts the `select` expression (the part before the method call)
   - Applies the transformation to just the current method
   - Rebuilds the chain with the transformed method

### Example Implementation

```java
@Override
public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
  J.MethodInvocation m = super.visitMethodInvocation(method, ctx);

  // Check if this is onFailure().recoverWithItem()
  if (m.getSelect() instanceof J.MethodInvocation onFailureInvocation &&
      isOnFailureMethod(onFailureInvocation)) {

    // Extract the mono expression (everything before .onFailure())
    Expression monoExpression = getMonoExpression(onFailureInvocation);

    // Transform: mono.onFailure().recoverWithItem(x) → mono.onErrorReturn(x)
    String methodName = m.getName().getSimpleName();
    if (methodName.equals("recoverWithItem")) {
      Expression arg = m.getArguments().get(0);
      m = onErrorReturnTemplate.apply(getCursor(), m.getCoordinates().replace(), monoExpression, arg);
    }
  }

  return m;
}
```

### Multiple Transformations

When multiple recipes are applied (e.g., `UniToMono` + `UniOnFailureToMono`), OpenRewrite:

1. Applies the first recipe (e.g., `UniToMono`)
2. Updates the AST with the changes
3. Applies the second recipe (e.g., `UniOnFailureToMono`) to the updated AST

This ensures that transformations compose correctly.

## Test Coverage

The project includes comprehensive tests for chained calls:

### ChainedTransformationsTest
- Tests single recipe transformations on chained calls
- 7 test cases covering various chain patterns

### CombinedRecipesTest
- Tests multiple recipes applied together
- 10 test cases covering complex migration scenarios
- Verifies that `Uni` → `Mono` and error handling transformations work together

## Key Design Principles

1. **Composability**: Each recipe focuses on a specific transformation pattern
2. **Independence**: Recipes don't need to know about each other
3. **Preservation**: Recipes preserve the parts of the chain they don't transform
4. **Correctness**: The visitor pattern ensures all matching patterns are transformed

## Supported Patterns

### Single Chain Transformations
- ✅ `Uni.createFrom().item("x").onFailure().recoverWithItem("y")`
- ✅ `Uni.createFrom().item("x").onFailure().retry().atMost(3)`
- ✅ `Uni.createFrom().failure(e).onFailure().invoke(handler)`

### Multiple Chain Transformations
- ✅ Multiple `.onFailure()` operators in sequence
- ✅ Complex chains with 3+ transformations
- ✅ Nested Uni/Mono creations

### Combined Recipe Transformations
- ✅ `Uni.createFrom().*` + `.onFailure().*` → Full Mono migration
- ✅ Type changes: `Uni<T>` → `Mono<T>`
- ✅ Import management: Adding Reactor imports
