using Flow.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Flow
{
    public interface IExecutionContext : ILogger
    {

        IState Scope { get; }
        IState Session { get; }

        IExecutionContext New();
        IExecutionContext New(IValueSource result);
        IValueSource Result { get; }
        CancellationToken CancellationToken { get; }
    }

    public interface IState : IEnumerable<KeyValuePair<string, object>>
    {
        object this[string name] { get; set; }

        IDictionary<string, object> GetState();
    }
}
