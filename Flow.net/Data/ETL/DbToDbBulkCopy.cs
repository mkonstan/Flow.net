using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace Flow.Data.ETL
{
    public abstract class DbToDbBulkCopy<TSource, TDest> : PipelineAction
        where TSource : IDbConnection, new()
        where TDest : IDbConnection, new()
    {
        public string SourceConnectionString { get; set; }
        public string SourceQuery { get; set; }
        public string DestinationConnectionString { get; set; }
        public string DestinationTable { get; set; }
        public IList<ColumnMapping> ColumnMappings { get; set; }
        public int BatchSize { get; set; } = 50000;
        public int CommandTimeout { get; set; } = 0;
        public int NotifyAfter { get; set; } = 100000;

        protected static TConn CreateConnection<TConn>(string connectionString)
            where TConn : IDbConnection, new()
            => new TConn { ConnectionString = connectionString };

        protected abstract Task<IList<ColumnMapping>> ResolveColumnMappingsAsync(
            IDbConnection destConn, string destinationTable, IDataReader sourceReader);

        protected abstract Task<long> TransferDataAsync(
            IDataReader reader, IDbConnection destConn, IList<ColumnMapping> mappings,
            TransferContext context);

        protected virtual Task OnBatchCompleteAsync(IExecutionContext context, long totalRows)
            => Task.CompletedTask;

        protected static (string Schema, string Table) ParseDestinationTable(string destinationTable)
        {
            var parts = destinationTable.Split('.');
            return parts.Length >= 2
                ? (parts[0], parts[1])
                : ("public", parts[0]);
        }

        protected static int[] PreComputeOrdinals(IDataReader reader, IList<ColumnMapping> mappings)
        {
            var ordinals = new int[mappings.Count];
            for (int i = 0; i < mappings.Count; i++)
                ordinals[i] = reader.GetOrdinal(mappings[i].SourceColumn);
            return ordinals;
        }

        protected override async Task<IValueSource> DefaultHandlerAsync(IExecutionContext context, IValueSource input)
        {
            var sourceConnStr = Format(SourceConnectionString, context, input, this);
            var sourceQuery = Format(SourceQuery, context, input, this);
            var destConnStr = Format(DestinationConnectionString, context, input, this);
            var destTable = Format(DestinationTable, context, input, this);

            using var sourceConn = CreateConnection<TSource>(sourceConnStr);
            using var reader = sourceConn.ExecuteReader(sourceQuery, commandTimeout: CommandTimeout);
            using var destConn = CreateConnection<TDest>(destConnStr);
            destConn.Open();

            var mappings = ColumnMappings;
            if (mappings == null || mappings.Count == 0)
                mappings = await ResolveColumnMappingsAsync(destConn, destTable, reader);

            var transferContext = new TransferContext
            {
                ExecutionContext = context,
                DestinationTable = destTable,
                BatchSize = BatchSize,
                NotifyAfter = NotifyAfter,
                CommandTimeout = CommandTimeout,
                OnBatchComplete = OnBatchCompleteAsync
            };

            long totalRows = 0;
            try
            {
                totalRows = await TransferDataAsync(reader, destConn, mappings, transferContext);
            }
            catch (Exception ex) when (!(ex is BulkCopyException))
            {
                throw new BulkCopyException(destTable, totalRows, ex.Message, ex);
            }

            await context.LogInfoAsync($"Bulk copy complete: {totalRows} total rows copied to {destTable}");
            return new ValuePrimitive<long>(totalRows);
        }
    }
}
