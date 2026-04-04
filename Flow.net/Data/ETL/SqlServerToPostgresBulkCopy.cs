using Dapper;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace Flow.Data.ETL
{
    public class SqlServerToPostgresBulkCopy : DbToDbBulkCopy
    {
        protected override DbConnection CreateSourceConnection(string connectionString)
            => new SqlConnection(connectionString);

        protected override NpgsqlConnection CreateDestinationConnection(string connectionString)
            => new NpgsqlConnection(connectionString);

        protected override async Task<(IList<ColumnMapping> Mappings, Dictionary<string, string> UdtNames)>
            ResolveColumnMappingsAsync(NpgsqlConnection destConn, string schema, string table, DbDataReader sourceReader)
        {
            var destColumns = await destConn.QueryAsync<(string column_name, string udt_name)>(
                @"SELECT column_name, udt_name FROM information_schema.columns
                  WHERE table_schema = @schema AND table_name = @table
                    AND is_generated = 'NEVER'
                  ORDER BY ordinal_position",
                new { schema, table });

            var sourceColumnSet = Enumerable.Range(0, sourceReader.FieldCount)
                .Select(i => sourceReader.GetName(i))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var mappings = new List<ColumnMapping>();
            var udtNames = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            foreach (var (column_name, udt_name) in destColumns)
            {
                if (sourceColumnSet.Contains(column_name))
                {
                    mappings.Add(new ColumnMapping(column_name, column_name));
                    udtNames[column_name] = udt_name;
                }
            }

            if (mappings.Count == 0)
                throw new BulkCopyException($"{schema}.{table}", 0,
                    "Auto-map found no matching columns between source and destination.", null);

            return (mappings, udtNames);
        }

        protected override async Task WriteRowAsync(
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
    }
}
