using Dapper;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Flow.Data.ETL;

namespace Flow.Data.Postgres
{
    public class PostgresToPostgresBulkCopy : DbToDbBulkCopy<NpgsqlConnection, NpgsqlConnection>
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
                new { schema, table });

            var sourceColumnSet = new HashSet<string>(
                Enumerable.Range(0, sourceReader.FieldCount).Select(i => sourceReader.GetName(i)),
                StringComparer.OrdinalIgnoreCase);

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

            var commandTimeout = CommandTimeout;
            var udtNames = await SqlServerToPostgresBulkCopy.QueryUdtNamesAsync(pgConn, schema, table, commandTimeout);
            var resolvedTypes = SqlServerToPostgresBulkCopy.ResolveColumnTypes(dbReader, mappings, ordinals, udtNames, context.DestinationTable);

            var copyCommand = SqlServerToPostgresBulkCopy.BuildCopyCommand(context.DestinationTable, mappings);

            long totalRows = 0;
            NpgsqlBinaryImporter writer = null;
            var importerTimeout = TimeSpan.FromSeconds(commandTimeout);
            try
            {
                writer = await pgConn.BeginBinaryImportAsync(copyCommand);
                writer.Timeout = importerTimeout;

                while (await dbReader.ReadAsync())
                {
                    await writer.StartRowAsync();
                    for (int i = 0; i < ordinals.Length; i++)
                    {
                        if (dbReader.IsDBNull(ordinals[i]))
                            await writer.WriteNullAsync();
                        else
                            await writer.WriteAsync(dbReader.GetValue(ordinals[i]), resolvedTypes[i]);
                    }
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
    }
}
