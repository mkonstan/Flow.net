using Flow.Logging;
using System;
using System.Collections.Concurrent;
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
            : this(logger, new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase), NullResult.Instance)
        { }

        private ExecutionContext(ILogger logger, IExecutionContext context, IPayload result)
            : this(logger, context.GetState(), result)
        { }

        private ExecutionContext(ILogger logger, IDictionary<string, object> state, IPayload result)
        {
            _logger = logger;
            _state = new ConcurrentDictionary<string, object>(state, StringComparer.OrdinalIgnoreCase);
            Result = result;
        }

        public object this[string name] { get => _state[name]; set => _state[name] = value; }

        public IPayload Result { get; private set; }

        public IDictionary<string, object> GetState() => new Dictionary<string, object>(
            _state,
            StringComparer.OrdinalIgnoreCase);

        public async Task LogErrorAsync(string message)
            => await Task.FromResult(_logger.LogErrorAsync(message));

        public async Task LogInfoAsync(string message)
            => await Task.FromResult(_logger.LogInfoAsync(message));

        public async Task LogWarningAsync(string message)
            => await Task.FromResult(_logger.LogWarningAsync(message));

        public IExecutionContext New()
            => New(NullResult.Instance);
        public IExecutionContext New(IPayload result)
            { return new ExecutionContext(_logger, this, result); }
    }
}
