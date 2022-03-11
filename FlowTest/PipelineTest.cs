using Flow;
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
            var builder = new PipelineBuilder(Logger);
            var result = await builder.StartWith<GetFiles>(op => {
                op.SearchPattern = "*.cs";
                op.DirectoryPath = Environment.CurrentDirectory;
                op.SearchOption = System.IO.SearchOption.AllDirectories;
            }).ExecuteAsync();
            Trace.WriteLine(JsonConvert.SerializeObject(result, Formatting.Indented));
            Assert.IsTrue(result is FilePathCollection);
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