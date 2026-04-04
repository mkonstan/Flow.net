# Flownet.Data.SqlServer

SQL Server data actions for the [Flownet](https://www.nuget.org/packages/Flownet) pipeline library.

## Actions

- **SqlServerExecute** - Execute SQL commands and stored procedures
- **SqlBulkLoadCsv** - Bulk load CSV files into SQL Server tables via SqlBulkCopy

## Usage

```csharp
var builder = new PipelineBuilder(logger);

await builder
    .StartWith<SqlServerExecute>(op =>
    {
        op.ConnectionString = "{session.ConnectionString}";
        op.Query = "TRUNCATE TABLE dbo.MyTable";
    })
    .ContinueWith<SqlBulkLoadCsv>(op =>
    {
        op.ConnectionString = "{session.ConnectionString}";
        op.DestinationTableName = "dbo.MyTable";
        op.Columns = new[] { "id", "name", "value" };
    })
    .ExecuteAsync();
```

## Installation

```
dotnet add package Flownet.Data.SqlServer
```

Requires [Flownet](https://www.nuget.org/packages/Flownet) (referenced automatically).
