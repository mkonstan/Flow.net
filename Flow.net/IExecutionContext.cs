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

        CancellationTokenSource TokenSource { get; }
        CancellationToken Token { get; }

        IExecutionContext New();
        IExecutionContext New(IValue result);
        IValue Result { get; }
    }
}
