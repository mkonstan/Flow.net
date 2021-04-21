using Flow.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Flow
{
    class ExecutionContext : IExecutionContext
    {
        private readonly ILogger _logger;
        private readonly IDictionary<string, object> _state;

        public ExecutionContext(ILogger logger)
            : this(logger, new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase))
        {
        }

        private ExecutionContext(ILogger logger, IExecutionContext context)
            : this(logger, context.GetState())
        {
        }

        private ExecutionContext(ILogger logger, IDictionary<string, object> state)
        {
            _logger = logger;
            _state = new System.Collections.Concurrent.ConcurrentDictionary<string, object>(state, StringComparer.OrdinalIgnoreCase);
        }

        public object this[string name] { get => _state[name]; set => _state[name] = value; }

        public IDictionary<string, object> GetState() => new Dictionary<string, object>(
            _state,
            StringComparer.OrdinalIgnoreCase);

        public async Task LogErrorAsync(string message)
            => await Task.FromResult(_logger.LogErrorAsync(message));

        public async Task LogInfoAsync(string message)
            => await Task.FromResult(_logger.LogInfoAsync(message));

        public async Task LogWarningAsync(string message)
            => await Task.FromResult(_logger.LogWarningAsync(message));

        public IExecutionContext New() { return new ExecutionContext(_logger, this); }
    }
}
