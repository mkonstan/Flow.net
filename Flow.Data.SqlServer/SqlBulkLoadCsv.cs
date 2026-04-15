using CsvHelper;
using CsvHelper.Configuration;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Flow.Data;

namespace Flow.Data.SqlServer
{
    public class SqlBulkLoadCsv : PipelineAction
    {
        public SqlBulkLoadCsv()
        {
            SetTypeHandler<FilePath>(async (context, input) => await HandlerAsync(this, context, input));
            SetTypeHandler<FilePathCollection>(async (context, input) => await HandlerAsync(this, context, input));
        }

        public int BulkCopyTimeout { get; set; } = 3000;
        public int BatchSize { get; set; } = 5000;
        public string DestinationTableName { get; set; }
        public string ConnectionString { get; set; }
        public string[] Columns { get; set; }

        protected SqlConnection CreateConnection(string connectionString) { return new Microsoft.Data.SqlClient.SqlConnection(connectionString); }

        protected static async Task<IValueSource> HandlerAsync(SqlBulkLoadCsv that, IExecutionContext context, FilePathCollection files)
        { return await HandlerAsync(that, context, files.Cast<FilePath>().ToArray()); }

        protected static async Task<IValueSource> HandlerAsync(SqlBulkLoadCsv that, IExecutionContext context, params FilePath[] files)
        { return await HandlerAsync(that, context, files.Select(p => p.Path).ToArray()); }

        protected static async Task<IValueSource> HandlerAsync(SqlBulkLoadCsv that, IExecutionContext context, IEnumerable<string> input)
        {
            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = true,
                PrepareHeaderForMatch = args => args.Header.ToLower()
            };
            var destinationColumns = that.Columns.ToHashSet(StringComparer.OrdinalIgnoreCase);
            var timer = new System.Diagnostics.Stopwatch();
            using (var conn = that.CreateConnection(Format(that.ConnectionString, context, null, that)))
            {
                await conn.OpenAsync();
                foreach (var file in input)
                {
                    var fileName = Path.GetFileName(file);
                    var sourceColumns = file.CsvReaderHeader(config).ToHashSet(StringComparer.OrdinalIgnoreCase);
                    var difference = destinationColumns.Except(sourceColumns).ToArray();
                    if (difference.Any())
                    {
                        throw new InvalidOperationException(
                            $"The given {difference.Serialize()} destination columns do not exist in {sourceColumns.Serialize()} source columns.");
                    }

                    difference = sourceColumns.Except(destinationColumns).ToArray();
                    if (difference.Any())
                    {
                        await context.LogWarningAsync($"The given {difference.Serialize()} source columns do not exist in {destinationColumns.Serialize()} destination columns.");
                    }

                    await context.LogInfoAsync($"[{fileName}] headers: {JsonConvert.SerializeObject(sourceColumns)}");
                    timer.Start();
                    using (var sqlCopy = new SqlBulkCopy(conn))
                    {
                        sqlCopy.DestinationTableName = that.DestinationTableName;
                        sqlCopy.BatchSize = that.BatchSize;
                        sqlCopy.NotifyAfter = 1000000;
                        sqlCopy.BulkCopyTimeout = that.BulkCopyTimeout;

                        sqlCopy.SqlRowsCopied += async (sender, e) =>
                        {
                            await context.LogInfoAsync($"{fileName}: {e.RowsCopied} records processed in { timer.Elapsed.TotalMinutes:N2}");
                        };

                        foreach (var column in that.Columns.Select(c => Format(c, context, null, that)))
                        {
                            sqlCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(column, column));
                        }
                        using (var csvReader = file.OpenCsvReader(config))
                        {
                            using (var dtReader = new CsvDataReaderExt(csvReader))
                            {
                                await sqlCopy.WriteToServerAsync(dtReader);
                                await context.LogInfoAsync($"{fileName}: all records processed in {timer.Elapsed.TotalMinutes:N2}");
                            }
                        }
                    }
                    timer.Stop();
                }
            }
            return NullResult.Instance;
        }
    }
}
