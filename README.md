# Flownet

A .NET pipeline orchestration library with fluent builder, parallel execution, type dispatch, and ETL support.

## Features

- **Fluent builder API** - `StartWith<T>().ContinueWith<T>().ExecuteAsync()`
- **Parallel execution** - `ParallelPipeline` (parallel actions) and `ParallelForEach` (parallel over elements)
- **Composable** - pipelines are actions, enabling nesting and reuse
- **Type dispatch** - polymorphic handler selection based on input type
- **Dual-scope state** - Session (shared) and Scope (copied per step)
- **Template formatting** - SmartFormat-based `{session.Variable}` resolution
- **ETL base class** - `DbToDbBulkCopy<TSource, TDest>` for database-to-database transfers
- **Exception hierarchy** - `FlowException`, `ParallelPipelineException`, `BulkCopyException`
- **Configurable resilience** - per-action `IErrorHandler` with built-in `RetryHandler` (Polly-backed) and `ContinueHandler`, plus optional fallback recovery pipelines
- **Side-channel `OnResult` pipelines** - attach a pipeline to any action to run telemetry / notifications / audit steps on successful results without mutating the main flow

## Quick Start

```csharp
var builder = new PipelineBuilder(logger);

await builder
    .StartWith<SetSessionVariables>(op =>
        op.AddStateVariable("OutputDir", @"C:\temp"))
    .ContinueWith<GetFiles>(op =>
    {
        op.DirectoryPath = "{session.OutputDir}";
        op.SearchPattern = "*.csv";
    })
    .ContinueWith<ForEach>(op =>
    {
        op.Actions = new[] { PipelineBuilder.CreateAction<ProcessFile>() };
    })
    .ExecuteAsync();
```

## Packages

| Package | Target | Description |
|---------|--------|-------------|
| [Flownet](https://www.nuget.org/packages/Flownet) | netstandard2.0 / net8.0 | Core pipeline engine, IO actions, base data classes |
| [Flownet.Data.SqlServer](https://www.nuget.org/packages/Flownet.Data.SqlServer) | netstandard2.0 / net8.0 | SQL Server actions (SqlServerExecute, SqlBulkLoadCsv) |
| [Flownet.Data.Postgres](https://www.nuget.org/packages/Flownet.Data.Postgres) | net8.0 | PostgreSQL actions (PostgresQLExecute, SqlServerToPostgresBulkCopy) |

## Installation

```bash
dotnet add package Flownet                    # Core
dotnet add package Flownet.Data.SqlServer     # SQL Server actions
dotnet add package Flownet.Data.Postgres      # PostgreSQL actions
```

## Resilience

Attach an `IErrorHandler` to any `IPipelineAction` to configure failure behavior. Default (null) preserves current fail-fast semantics — zero migration cost.

```csharp
// Continue on error — log and return NullResult
new HttpDownload { Url = "...", ErrorHandler = new ContinueHandler() }

// Retry with exponential backoff, then give up
new HttpDownload
{
    Url = "...",
    ErrorHandler = new RetryHandler { MaxAttempts = 3 }
}

// Retry, then run a fallback recovery pipeline (e.g., dead-letter)
new ProcessRecord
{
    ErrorHandler = new RetryHandler
    {
        MaxAttempts = 3,
        Pipeline = new Pipeline { Actions = new IPipelineAction[]
        {
            new WriteToDeadLetterQueue(),
            new ReturnNull()
        }}
    }
}
```

`OperationCanceledException` is non-recoverable — it bypasses all handlers. Implement `IErrorHandler` directly for custom strategies (circuit breaker, bulkhead, telemetry hooks).

## OnResult Side-Channel Pipelines

Attach an `OnResult: IPipeline` to any `IPipelineAction` to run follow-up steps (telemetry, notifications, audit) AFTER the action succeeds. The action's declared return type is preserved — OnResult pipelines cannot mutate the main flow's data.

```csharp
// Emit metrics on each successful download
new HttpDownload
{
    Url = "...",
    OnResult = new Pipeline { Actions = new IPipelineAction[]
    {
        new LogMetrics { MetricName = "download_size" }
    }}
}

// Fan-out side-chain — index each file as a side-effect
new GetFiles
{
    DirectoryPath = "...",
    OnResult = new Pipeline { Actions = new IPipelineAction[]
    {
        new ForEach { Actions = new[] { new IndexFile() } }
    }}
}
// Still returns FilePathCollection to the main flow
```

**Semantics:**

- Fires ONLY on genuine action success — not on `ErrorHandler`-recovered values (`ContinueHandler` fallback, `RetryHandler` exhaustion fallback pipelines).
- Returns the action's ORIGINAL result. OnResult pipeline output is discarded.
- Exceptions from OnResult are logged as warnings and swallowed. The action still returns its result. Exception: `OperationCanceledException` propagates.
- Runs with a **shallow-isolated** context: Scope and Session dictionaries are cloned (key reassignments don't leak back), but mutable reference values inside them remain shared with the parent. Store immutable values in Scope/Session if full isolation matters.

## Changelog

### 0.3.0 (2026-04-16)

**Features**
- Added `OnResult: IPipeline` property on `IPipelineAction` for side-channel follow-up pipelines (telemetry, notifications, audit). Fires only on genuine action success; never on `ErrorHandler`-recovered values. Returns the action's original result; `OnResult` pipeline output is discarded.
- `OnResult` pipelines run with a shallow-isolated context (see README section for details).

**Compatibility**
- **Behaviorally additive** for actions deriving from `PipelineAction` — null default means no runtime behavior change.
- **Source-breaking for consumers that implement `IPipelineAction` directly** — the interface gains a new required `OnResult` property. In practice all in-repo implementors derive from `PipelineAction`; external consumers who wrote custom `IPipelineAction` implementations must add the property.

### 0.2.0 (2026-04-16)

**Features**
- Added per-`IPipelineAction` resilience via `IErrorHandler`. Built-ins: `RetryHandler` (Polly v8-backed), `ContinueHandler`. Default null = current behavior (zero migration).
- Added `CancellationToken` to `IExecutionContext` (non-breaking; defaults to `CancellationToken.None`).
- Added `ErrorPayload` for fallback recovery pipelines.

**Breaking**

### 0.2.0 (2026-04-16)

**Features**
- Added per-`IPipelineAction` resilience via `IErrorHandler`. Built-ins: `RetryHandler` (Polly v8-backed), `ContinueHandler`. Default null = current behavior (zero migration).
- Added `CancellationToken` to `IExecutionContext` (non-breaking; defaults to `CancellationToken.None`).
- Added `ErrorPayload` for fallback recovery pipelines.

**Breaking**
- `Flownet.Data.SqlServer` and `Flownet.Data.Postgres` now use `Microsoft.Data.SqlClient` (5.2.2) instead of `System.Data.SqlClient`. `Microsoft.Data.SqlClient` defaults `Encrypt=true` — connection strings to non-TLS SQL Server instances may need `Encrypt=false` or `TrustServerCertificate=true`.
- Removed unused `ErrorResult` type (was dead code — protected constructor, unusable externally).

**Dependencies**
- `Flownet` now depends on `Polly.Core` 8.6.6.

## License

MIT
