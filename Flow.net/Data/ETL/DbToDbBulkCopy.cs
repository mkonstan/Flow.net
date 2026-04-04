using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace Flow.Data.ETL
{
    public abstract class DbToDbBulkCopy : PipelineAction
    {
        public string SourceConnectionString { get; set; }
        public string SourceQuery { get; set; }
        public string DestinationConnectionString { get; set; }
        public string DestinationTable { get; set; }
        public IList<ColumnMapping> ColumnMappings { get; set; }
        public int BatchSize { get; set; } = 50000;
        public int? CommandTimeout { get; set; }
        public int NotifyAfter { get; set; } = 100000;

        protected abstract DbConnection CreateSourceConnection(string connectionString);
        protected abstract NpgsqlConnection CreateDestinationConnection(string connectionString);

        protected virtual Task<(IList<ColumnMapping> Mappings, Dictionary<string, string> UdtNames)>
            ResolveColumnMappingsAsync(NpgsqlConnection destConn, string schema, string table, DbDataReader sourceReader)
        {
            throw new ActionConfigurationException(
                GetType().Name,
                "ColumnMappings is empty and ResolveColumnMappingsAsync is not overridden. Provide explicit ColumnMappings or override ResolveColumnMappingsAsync.");
        }

        protected virtual string BuildCopyCommand(string destinationTable, IList<ColumnMapping> mappings)
        {
            var columns = string.Join(", ", mappings.Select(m => $"\"{m.DestinationColumn}\""));
            return $"COPY {destinationTable} ({columns}) FROM STDIN (FORMAT BINARY)";
        }

        protected abstract Task WriteRowAsync(
            DbDataReader reader, NpgsqlBinaryImporter writer, NpgsqlDbType[] resolvedTypes, int[] ordinals);

        protected virtual Task OnBatchCompleteAsync(IExecutionContext context, long totalRows)
            => Task.CompletedTask;

        protected static (string Schema, string Table) ParseDestinationTable(string destinationTable)
        {
            var parts = destinationTable.Split('.');
            return parts.Length >= 2
                ? (parts[0], parts[1])
                : ("public", parts[0]);
        }

        protected override async Task<IValueSource> DefaultHandlerAsync(IExecutionContext context, IValueSource input)
        {
            var sourceConnStr = Format(SourceConnectionString, context, input, this);
            var sourceQuery = Format(SourceQuery, context, input, this);
            var destConnStr = Format(DestinationConnectionString, context, input, this);
            var destTable = Format(DestinationTable, context, input, this);
            var (schema, table) = ParseDestinationTable(destTable);

            long totalRows = 0;

            await using var sourceConn = CreateSourceConnection(sourceConnStr);
            await sourceConn.OpenAsync();

            await using var cmd = sourceConn.CreateCommand();
            cmd.CommandText = sourceQuery;
            if (CommandTimeout.HasValue)
                cmd.CommandTimeout = CommandTimeout.Value;

            await using var reader = await cmd.ExecuteReaderAsync();
            await using var destConn = CreateDestinationConnection(destConnStr);
            await destConn.OpenAsync();

            var mappings = ColumnMappings;
            Dictionary<string, string> udtNames = null;

            if (mappings == null || mappings.Count == 0)
            {
                var result = await ResolveColumnMappingsAsync(destConn, schema, table, reader);
                mappings = result.Mappings;
                udtNames = result.UdtNames;
            }

            var ordinals = PreComputeOrdinals(reader, mappings);
            var needsSchemaQuery = NeedsSchemaQuery(reader, mappings, ordinals);

            if (needsSchemaQuery && udtNames == null)
                udtNames = await QueryUdtNamesAsync(destConn, schema, table);

            var resolvedTypes = ResolveColumnTypes(reader, mappings, ordinals, udtNames, destTable);
            var copyCommand = BuildCopyCommand(destTable, mappings);

            NpgsqlBinaryImporter writer = null;
            try
            {
                writer = await destConn.BeginBinaryImportAsync(copyCommand);

                while (await reader.ReadAsync())
                {
                    await writer.StartRowAsync();
                    await WriteRowAsync(reader, writer, resolvedTypes, ordinals);
                    totalRows++;

                    if (BatchSize > 0 && totalRows % BatchSize == 0)
                    {
                        await writer.CompleteAsync();
                        await writer.DisposeAsync();
                        await context.LogInfoAsync($"Batch complete: {totalRows} rows committed to {destTable}");
                        await OnBatchCompleteAsync(context, totalRows);
                        writer = await destConn.BeginBinaryImportAsync(copyCommand);
                    }
                    else if (NotifyAfter > 0 && totalRows % NotifyAfter == 0)
                    {
                        await context.LogInfoAsync($"Progress: {totalRows} rows streamed to {destTable}");
                    }
                }

                await writer.CompleteAsync();
                await writer.DisposeAsync();
                writer = null;
            }
            catch (PostgresException ex)
            {
                throw new BulkCopyException(destTable, totalRows, ex.MessageText, ex);
            }
            catch (Exception ex) when (ex is not BulkCopyException)
            {
                throw new BulkCopyException(destTable, totalRows, ex.Message, ex);
            }
            finally
            {
                if (writer != null)
                    await writer.DisposeAsync();
            }

            await context.LogInfoAsync($"Bulk copy complete: {totalRows} total rows copied to {destTable}");
            return new ValuePrimitive<long>(totalRows);
        }

        private static int[] PreComputeOrdinals(DbDataReader reader, IList<ColumnMapping> mappings)
        {
            var ordinals = new int[mappings.Count];
            for (int i = 0; i < mappings.Count; i++)
                ordinals[i] = reader.GetOrdinal(mappings[i].SourceColumn);
            return ordinals;
        }

        private static bool NeedsSchemaQuery(DbDataReader reader, IList<ColumnMapping> mappings, int[] ordinals)
        {
            for (int i = 0; i < mappings.Count; i++)
            {
                if (mappings[i].DestinationType.HasValue)
                    continue;

                var clrType = reader.GetFieldType(ordinals[i]);
                if (InferNpgsqlDbTypeFromClr(clrType) == null)
                    return true;
            }
            return false;
        }

        protected static NpgsqlDbType[] ResolveColumnTypes(
            DbDataReader reader, IList<ColumnMapping> mappings, int[] ordinals,
            Dictionary<string, string> udtNames, string destinationTable)
        {
            var resolved = new NpgsqlDbType[mappings.Count];
            for (int i = 0; i < mappings.Count; i++)
            {
                if (mappings[i].DestinationType.HasValue)
                {
                    resolved[i] = mappings[i].DestinationType.Value;
                    continue;
                }

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
                    $"(CLR type: {clrType.Name}). Provide an explicit DestinationType in ColumnMappings.", null);
            }
            return resolved;
        }

        protected static NpgsqlDbType? InferNpgsqlDbTypeFromClr(Type clrType) => clrType switch
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

        private static NpgsqlDbType ResolveAmbiguousType(Type clrType, string udtName, string destinationTable) =>
            udtName switch
            {
                "timestamp" => NpgsqlDbType.Timestamp,
                "timestamptz" => NpgsqlDbType.TimestampTz,
                "date" => NpgsqlDbType.Date,
                "text" => NpgsqlDbType.Text,
                "varchar" => NpgsqlDbType.Varchar,
                "bpchar" => NpgsqlDbType.Char,
                "xml" => NpgsqlDbType.Xml,
                "json" => NpgsqlDbType.Json,
                "jsonb" => NpgsqlDbType.Jsonb,
                _ => throw new BulkCopyException(destinationTable, 0,
                    $"Cannot resolve NpgsqlDbType for CLR type {clrType.Name} with destination udt_name '{udtName}'.", null)
            };

        private static async Task<Dictionary<string, string>> QueryUdtNamesAsync(NpgsqlConnection conn, string schema, string table)
        {
            var udtNames = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            await using var cmd = new NpgsqlCommand(
                "SELECT column_name, udt_name FROM information_schema.columns WHERE table_schema = @schema AND table_name = @table AND is_generated = 'NEVER'",
                conn);
            cmd.Parameters.AddWithValue("schema", schema);
            cmd.Parameters.AddWithValue("table", table);

            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                udtNames[reader.GetString(0)] = reader.GetString(1);

            return udtNames;
        }
    }
}
