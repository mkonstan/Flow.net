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

| Package | Description |
|---------|-------------|
| [Flownet](https://www.nuget.org/packages/Flownet) | Core pipeline engine, IO actions, base data classes |
| [Flownet.Data.SqlServer](https://www.nuget.org/packages/Flownet.Data.SqlServer) | SQL Server actions (SqlServerExecute, SqlBulkLoadCsv) |
| [Flownet.Data.Postgres](https://www.nuget.org/packages/Flownet.Data.Postgres) | PostgreSQL actions (PostgresQLExecute, SqlServerToPostgresBulkCopy) |

## Target Frameworks

- `netstandard2.0` - LINQPad and .NET Framework compatibility
- `net8.0` - Modern .NET (Flownet.Data.Postgres is net8.0 only)
