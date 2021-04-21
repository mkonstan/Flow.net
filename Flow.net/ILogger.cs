using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Flow.Logging
{
    public interface ILogger
    {
        Task LogErrorAsync(string message);

        Task LogInfoAsync(string message);

        Task LogWarningAsync(string message);
    }
}
