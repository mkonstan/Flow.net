using Flow.Logging;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Flow
{
    class ExecutionContext : IExecutionContext
    {
        private readonly ILogger _logger;

        public ExecutionContext(ILogger logger)
            : this(logger, new State(), new State(), NullResult.Instance)
        { }

        private ExecutionContext(ILogger logger, IExecutionContext context, IValueSource result)
            : this(logger, context.Scope, context.Session, result)
        { }

        private ExecutionContext(ILogger logger, IState scope, IState session, IValueSource result)
        {
            _logger = logger;
            Scope = new State(scope.GetState());
            Session = session;
            Result = result;
        }

        public object this[string name] { get => Scope[name]; set => Scope[name] = value; }

        public IValueSource Result { get; private set; }

        public IState Scope { get; }

        public IState Session { get; }

        public async Task LogErrorAsync(string message)
            => await _logger.LogErrorAsync(message);

        public async Task LogInfoAsync(string message)
            => await _logger.LogInfoAsync(message);

        public async Task LogWarningAsync(string message)
            => await _logger.LogWarningAsync(message);

        public IExecutionContext New()
            => New(NullResult.Instance);
        public IExecutionContext New(IValueSource result)
            { return new ExecutionContext(_logger, this, result); }

        class State : IState
        {
            private readonly IDictionary<string, object> _state;

            public State()
                :this(new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase))
            {}

            public State(IDictionary<string, object> state)
            {
                _state = new ConcurrentDictionary<string, object>(state, StringComparer.OrdinalIgnoreCase);
            }

            public object this[string name]
            {
                get => _state[name];
                set => _state[name] = value;
            }

            public IEnumerator<KeyValuePair<string, object>> GetEnumerator()
                => GetState().GetEnumerator();

            public IDictionary<string, object> GetState() => new Dictionary<string, object>(
                _state,
                StringComparer.OrdinalIgnoreCase);

            IEnumerator IEnumerable.GetEnumerator()
                => GetEnumerator();
        }
    }
}
