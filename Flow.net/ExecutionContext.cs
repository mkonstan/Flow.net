using Flow.Logging;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Flow
{
    class ExecutionContext : IExecutionContext
    {
        private readonly ILogger _logger;

        public ExecutionContext(ILogger logger, CancellationTokenSource tokenSource)
            : this(logger, new State(), new State(), NullResult.Instance, tokenSource)
        { }

        private ExecutionContext(ILogger logger, IExecutionContext context, IValue result)
            : this(logger, context.Scope, context.Session, result, context.TokenSource)
        { }

        private ExecutionContext(ILogger logger, IState scope, IState session, IValue result, CancellationTokenSource tokenSource)
        {
            _logger = logger;
            Scope = new State(scope.GetState());
            Session = session;
            Result = result;
            TokenSource = tokenSource;
        }

        public object this[string name] { get => Scope[name]; set => Scope[name] = value; }

        public IValue Result { get; private set; }

        public IState Scope { get; }

        public IState Session { get; }

        public System.Threading.CancellationTokenSource TokenSource { get; }

        public System.Threading.CancellationToken Token => TokenSource.Token;

        public async Task LogErrorAsync(string message)
            => await Task.FromResult(_logger.LogErrorAsync(message));

        public async Task LogInfoAsync(string message)
            => await Task.FromResult(_logger.LogInfoAsync(message));

        public async Task LogWarningAsync(string message)
            => await Task.FromResult(_logger.LogWarningAsync(message));

        public IExecutionContext New()
            => New(NullResult.Instance);
        public IExecutionContext New(IValue result)
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
