using Dapper;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Flow.Data.ETL;

[assembly: InternalsVisibleTo("FlowTest")]

namespace Flow.Data.Postgres
{
    public class SqlServerToPostgresBulkCopy : DbToDbBulkCopy<SqlConnection, NpgsqlConnection>
    {
        protected override async Task<IList<ColumnMapping>> ResolveColumnMappingsAsync(
            IDbConnection destConn, string destinationTable, IDataReader sourceReader)
        {
            var pgConn = (NpgsqlConnection)destConn;
            var (schema, table) = ParseDestinationTable(destinationTable);

            var destColumns = await pgConn.QueryAsync<(string column_name, string udt_name)>(
                @"SELECT column_name, udt_name FROM information_schema.columns
                  WHERE table_schema = @schema AND table_name = @table
                    AND is_generated = 'NEVER'
                  ORDER BY ordinal_position",
                new { schema, table },
                commandTimeout: CommandTimeout);

            var sourceColumnSet = Enumerable.Range(0, sourceReader.FieldCount)
                .Select(i => sourceReader.GetName(i))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var mappings = new List<ColumnMapping>();

            foreach (var (column_name, udt_name) in destColumns)
            {
                if (sourceColumnSet.Contains(column_name))
                    mappings.Add(new ColumnMapping(column_name, column_name));
            }

            if (mappings.Count == 0)
                throw new BulkCopyException($"{schema}.{table}", 0,
                    "Auto-map found no matching columns between source and destination.", null);

            return mappings;
        }

        protected override async Task<long> TransferDataAsync(
            IDataReader reader, IDbConnection destConn, IList<ColumnMapping> mappings,
            TransferContext context)
        {
            var pgConn = (NpgsqlConnection)destConn;
            var dbReader = (DbDataReader)reader;
            var (schema, table) = ParseDestinationTable(context.DestinationTable);
            var ordinals = PreComputeOrdinals(reader, mappings);

            var needsSchemaQuery = NeedsSchemaQuery(dbReader, mappings, ordinals);
            Dictionary<string, string> udtNames = null;
            if (needsSchemaQuery)
                udtNames = await QueryUdtNamesAsync(pgConn, schema, table, context.CommandTimeout);

            var resolvedTypes = ResolveColumnTypes(dbReader, mappings, ordinals, udtNames, context.DestinationTable);
            var copyCommand = BuildCopyCommand(context.DestinationTable, mappings);

            long totalRows = 0;
            NpgsqlBinaryImporter writer = null;
            var importerTimeout = context.CommandTimeout == 0
                ? TimeSpan.Zero
                : TimeSpan.FromSeconds(context.CommandTimeout);
            try
            {
                writer = await pgConn.BeginBinaryImportAsync(copyCommand);
                writer.Timeout = importerTimeout;

                while (await dbReader.ReadAsync())
                {
                    await writer.StartRowAsync();
                    await WriteRowAsync(dbReader, writer, resolvedTypes, ordinals);
                    totalRows++;

                    if (context.BatchSize > 0 && totalRows % context.BatchSize == 0)
                    {
                        await writer.CompleteAsync();
                        await writer.DisposeAsync();
                        await context.ExecutionContext.LogInfoAsync(
                            $"Batch complete: {totalRows} rows committed to {context.DestinationTable}");
                        await context.OnBatchComplete(context.ExecutionContext, totalRows);
                        writer = await pgConn.BeginBinaryImportAsync(copyCommand);
                        writer.Timeout = importerTimeout;
                    }
                    else if (context.NotifyAfter > 0 && totalRows % context.NotifyAfter == 0)
                    {
                        await context.ExecutionContext.LogInfoAsync(
                            $"Progress: {totalRows} rows streamed to {context.DestinationTable}");
                    }
                }

                await writer.CompleteAsync();
                await writer.DisposeAsync();
                writer = null;
            }
            catch (PostgresException ex)
            {
                throw new BulkCopyException(context.DestinationTable, totalRows, ex.MessageText, ex);
            }
            finally
            {
                if (writer != null)
                    await writer.DisposeAsync();
            }

            return totalRows;
        }

        internal static string BuildCopyCommand(string destinationTable, IList<ColumnMapping> mappings)
        {
            var columns = string.Join(", ", mappings.Select(m => $"\"{m.DestinationColumn}\""));
            return $"COPY {destinationTable} ({columns}) FROM STDIN (FORMAT BINARY)";
        }

        private static async Task WriteRowAsync(
            DbDataReader reader, NpgsqlBinaryImporter writer, NpgsqlDbType[] resolvedTypes, int[] ordinals)
        {
            for (int i = 0; i < ordinals.Length; i++)
            {
                var ordinal = ordinals[i];
                if (reader.IsDBNull(ordinal))
                {
                    await writer.WriteNullAsync();
                }
                else
                {
                    var value = reader.GetValue(ordinal);
                    await writer.WriteAsync(value, resolvedTypes[i]);
                }
            }
        }

        private static bool NeedsSchemaQuery(DbDataReader reader, IList<ColumnMapping> mappings, int[] ordinals)
        {
            for (int i = 0; i < mappings.Count; i++)
            {
                var clrType = reader.GetFieldType(ordinals[i]);
                if (InferNpgsqlDbTypeFromClr(clrType) == null)
                    return true;
            }
            return false;
        }

        internal static NpgsqlDbType[] ResolveColumnTypes(
            DbDataReader reader, IList<ColumnMapping> mappings, int[] ordinals,
            Dictionary<string, string> udtNames, string destinationTable)
        {
            var resolved = new NpgsqlDbType[mappings.Count];
            for (int i = 0; i < mappings.Count; i++)
            {
                var clrType = reader.GetFieldType(ordinals[i]);
                var inferred = InferNpgsqlDbTypeFromClr(clrType);
                if (inferred.HasValue)
                {
                    resolved[i] = inferred.Value;
                    continue;
                }

                if (udtNames != null && udtNames.TryGetValue(mappings[i].DestinationColumn, out var udtName))
                {
                    resolved[i] = ResolveAmbiguousType(clrType, udtName, destinationTable);
                    continue;
                }

                throw new BulkCopyException(destinationTable, 0,
                    $"Cannot resolve NpgsqlDbType for column '{mappings[i].SourceColumn}' " +
                    $"(CLR type: {clrType.Name}). Provide explicit ColumnMappings or ensure the destination table schema is queryable.", null);
            }
            return resolved;
        }

        internal static NpgsqlDbType? InferNpgsqlDbTypeFromClr(Type clrType) => clrType switch
        {
            _ when clrType == typeof(int) => NpgsqlDbType.Integer,
            _ when clrType == typeof(long) => NpgsqlDbType.Bigint,
            _ when clrType == typeof(short) => NpgsqlDbType.Smallint,
            _ when clrType == typeof(byte) => NpgsqlDbType.Smallint,
            _ when clrType == typeof(bool) => NpgsqlDbType.Boolean,
            _ when clrType == typeof(decimal) => NpgsqlDbType.Numeric,
            _ when clrType == typeof(double) => NpgsqlDbType.Double,
            _ when clrType == typeof(float) => NpgsqlDbType.Real,
            _ when clrType == typeof(DateTimeOffset) => NpgsqlDbType.TimestampTz,
            _ when clrType == typeof(TimeSpan) => NpgsqlDbType.Time,
            _ when clrType == typeof(Guid) => NpgsqlDbType.Uuid,
            _ when clrType == typeof(byte[]) => NpgsqlDbType.Bytea,
            _ when clrType == typeof(DateTime) => null,
            _ when clrType == typeof(string) => null,
            _ => null
        };

        internal static NpgsqlDbType ResolveAmbiguousType(Type clrType, string udtName, string destinationTable) =>
            udtName switch
            {
                "timestamp" => NpgsqlDbType.Timestamp,
                "timestamptz" => NpgsqlDbType.TimestampTz,
                "date" => NpgsqlDbType.Date,
                "text" => NpgsqlDbType.Text,
                "varchar" => NpgsqlDbType.Varchar,
                "bpchar" => NpgsqlDbType.Char,
                "citext" => NpgsqlDbType.Citext,
                "xml" => NpgsqlDbType.Xml,
                "json" => NpgsqlDbType.Json,
                "jsonb" => NpgsqlDbType.Jsonb,
                _ => throw new BulkCopyException(destinationTable, 0,
                    $"Cannot resolve NpgsqlDbType for CLR type {clrType.Name} with destination udt_name '{udtName}'.", null)
            };

        internal static async Task<Dictionary<string, string>> QueryUdtNamesAsync(NpgsqlConnection conn, string schema, string table, int commandTimeout)
        {
            var udtNames = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            await using var cmd = new NpgsqlCommand(
                "SELECT column_name, udt_name FROM information_schema.columns WHERE table_schema = @schema AND table_name = @table AND is_generated = 'NEVER'",
                conn);
            cmd.CommandTimeout = commandTimeout;
            cmd.Parameters.AddWithValue("schema", schema);
            cmd.Parameters.AddWithValue("table", table);

            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                udtNames[reader.GetString(0)] = reader.GetString(1);

            return udtNames;
        }
    }
}
