using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Polly;
using Polly.Fallback;
using Polly.Utilities;

namespace Flow.Policy
{
    class PipelineException : Exception
    {
        public PipelineException(IPipelineAction action, Exception ex)
            :this(action, ex.Message, ex)
        {
        }

        public PipelineException(IPipelineAction action, string message, Exception ex)
            : base(message, ex)
        {
            Action = action;
        }

        public IPipelineAction Action { get; }
    }

    class FatalException : PipelineException
    {
        public FatalException(IPipelineAction action, string message, Exception ex)
            : base(action, message, ex)
        { }

        public FatalException(IPipelineAction action, Exception ex)
            : base(action, ex)
        { }
    }

    public interface IExecutionPolicy
    {
        IAsyncPolicy<IValue> CreatePolicy(PipelineAction action, IExecutionContext context, IValue input);
    }

    public abstract class ExecutionPolicy : IExecutionPolicy
    {
        protected static readonly PolicyBuilder<IValue> PolicyBuilder = Policy<IValue>
            .Handle<Exception>(ex =>
            {
                return !(ex is FatalException);
            });

        public abstract IAsyncPolicy<IValue> CreatePolicy(PipelineAction action, IExecutionContext context, IValue input);
    }

    public sealed class DefaultPolicy : ExecutionPolicy
    {
        public override IAsyncPolicy<IValue> CreatePolicy(PipelineAction action, IExecutionContext context, IValue input)
        {
            var fallfack = Policy<IValue>.Handle<Exception>().FallbackAsync((result, _, ctx) =>
            {
                var exception = result.Exception;
                if (exception is PipelineException) throw exception;
                throw new PipelineException(action, exception.Message, exception);
            });

            return Polly.Policy.WrapAsync(Polly.Policy.NoOpAsync<IValue>(), fallfack);
        }
    }

    static class PollyHelpers
    {
        public static AsyncFallbackPolicy<TResult> FallbackAsync<TResult>(
            this PolicyBuilder<TResult> policyBuilder,
            Func<DelegateResult<TResult>, Context, CancellationToken, Task<TResult>> fallbackAction)
        {
            if (fallbackAction == null)
            {
                throw new ArgumentNullException("fallbackAction");
            }
            Func<DelegateResult<TResult>, Context, Task> doNothing = (result, ctx) => TaskHelper.EmptyTask;
            return policyBuilder.FallbackAsync(fallbackAction, doNothing);
        }
    }
}
