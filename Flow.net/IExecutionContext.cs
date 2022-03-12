using Flow.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Flow
{
    public interface IExecutionContext : ILogger
    {

        IState Scope { get; }
        IState Session { get; }

        IExecutionContext New();
        IExecutionContext New(IPayload result);
        IPayload Result { get; }
    }

    public interface IState : IEnumerable<KeyValuePair<string, object>>
    {
        object this[string name] { get; set; }

        IDictionary<string, object> GetState();
    }
}
