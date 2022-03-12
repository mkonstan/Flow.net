using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Flow;
using Flow.Logging;

namespace FlowTest
{

    public class TraceLogger : ILogger
    {
        public async Task LogErrorAsync(string message)
                => await Task.Run(() => Trace.TraceError(message));

        public async Task LogInfoAsync(string message)
                => await Task.Run(() => Trace.TraceInformation(message));

        public async Task LogWarningAsync(string message)
                => await Task.Run(() => Trace.TraceWarning(message));
    }
}
