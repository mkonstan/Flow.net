using Flow.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

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

        public void LogError(string message) => _logger.LogError(message);

        public void LogInfo(string message) => _logger.LogInfo(message);

        public void LogWarning(string message) => _logger.LogWarning(message);

        public IExecutionContext New() { return new ExecutionContext(_logger, this); }
    }
}
