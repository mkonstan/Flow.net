using System;
using System.Collections.Generic;
using System.Text;

namespace Flow.Logging
{
    public interface ILogger
    {
        void LogError(string message);

        void LogInfo(string message);

        void LogWarning(string message);
    }
}
