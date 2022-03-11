using Flow;
using Flow.Data.SqlServer;
using Flow.IO;
using Flow.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace FlowTest
{
    [TestClass]
    public class PipelineTest
    {
        protected static readonly ILogger Logger = new TraceLogger();

        [TestMethod]
        public async Task RunTest()
        {
            new SqlBulkLoadCsv() { };
            var builder = new PipelineBuilder(Logger);
            var result = await builder.StartWith<GetFiles>(op =>
                {
                    op.SearchPattern = "*.cs";
                    op.DirectoryPath = @"C:\Projects\Flow.net - GitHub";
                    op.SearchOption = System.IO.SearchOption.AllDirectories;
                })
                .ContinueWith<StoreInScope>(op =>
                {
                    op.Name = "FileCollection";
                })
                .ContinueWith<ForEach>(op =>
                {
                    op.PayloadProvider = new GetScopedPayload { Name = "FileCollection" };
                    op.Actions = builder.StartWith<LogResult>().Create().Actions;
                })
                .ContinueWith<LogContext>()
                .ExecuteAsync();
            Trace.WriteLine(JsonConvert.SerializeObject(result, Formatting.Indented));
            Assert.IsTrue(result is PayloadCollection);
        }

        class TraceLogger : ILogger
        {
            public async Task LogErrorAsync(string message)
                    => await Task.Run(() => Trace.TraceError(message));

            public async Task LogInfoAsync(string message)
                    => await Task.Run(() => Trace.TraceInformation(message));

            public async Task LogWarningAsync(string message)
                    => await Task.Run(() => Trace.TraceWarning(message));
        }
    }
}