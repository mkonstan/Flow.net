# Flownet.Data.Postgres

PostgreSQL data actions for the [Flownet](https://www.nuget.org/packages/Flownet) pipeline library.

## Actions

- **PostgresQLExecute** - Execute SQL commands against PostgreSQL via Dapper
- **SqlServerToPostgresBulkCopy** - Stream data from SQL Server to PostgreSQL via Npgsql binary import

## SqlServerToPostgresBulkCopy

High-performance bulk data transfer using Npgsql's binary COPY protocol. Features:

- **Two-layer type resolution** - CLR type inference with destination schema tiebreaker
- **Auto-mapping** - matches source/destination columns by name
- **Batched commits** - configurable batch size with progress logging
- **Generated column exclusion** - automatically skips computed columns

```csharp
var builder = new PipelineBuilder(logger);

await builder
    .StartWith<SqlServerToPostgresBulkCopy>(op =>
    {
        op.SourceConnectionString = "Server=.;Database=MyDb;...";
        op.SourceQuery = "SELECT * FROM dbo.MyTable";
        op.DestinationConnectionString = "Host=localhost;Database=mydb;...";
        op.DestinationTable = "public.mytable";
        op.BatchSize = 100000;
        op.NotifyAfter = 500000;
    })
    .ExecuteAsync();
```

## Installation

```
dotnet add package Flownet.Data.Postgres
```

Requires [Flownet](https://www.nuget.org/packages/Flownet) (referenced automatically). Target: net8.0 only.
