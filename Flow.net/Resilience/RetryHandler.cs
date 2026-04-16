using Polly;
using Polly.Retry;
using System;
using System.Threading.Tasks;

namespace Flow
{
    public sealed class RetryHandler : IErrorHandler
    {
        public int MaxAttempts { get; set; } = 3;
        public TimeSpan InitialBackoff { get; set; } = TimeSpan.FromMilliseconds(100);
        public double BackoffMultiplier { get; set; } = 2.0;
        public Func<Exception, bool> ShouldRetry { get; set; } = _ => true;
        public IPipeline Pipeline { get; set; }

        public async Task<IValueSource> HandledActionAsync(
            IExecutionContext context, IPipelineAction action, IValueSource originalInput, Func<Task<IValueSource>> work)
        {
            var pipeline = new ResiliencePipelineBuilder<IValueSource>()
                .AddRetry(new RetryStrategyOptions<IValueSource>
                {
                    MaxRetryAttempts = MaxAttempts - 1,
                    ShouldHandle = new PredicateBuilder<IValueSource>()
                        .Handle<Exception>(ex =>
                            !(ex is OperationCanceledException) && ShouldRetry(ex)),
                    DelayGenerator = args =>
                    {
                        var delay = TimeSpan.FromMilliseconds(
                            InitialBackoff.TotalMilliseconds *
                            Math.Pow(BackoffMultiplier, args.AttemptNumber));
                        return new ValueTask<TimeSpan?>(delay);
                    },
                    OnRetry = args =>
                    {
                        return new ValueTask(context.LogInfoAsync(
                            $"Retry {args.AttemptNumber + 1}/{MaxAttempts} for " +
                            $"{action.GetType().Name} after {args.Outcome.Exception?.Message} " +
                            $"(waiting {args.RetryDelay.TotalMilliseconds}ms)"));
                    }
                })
                .Build();

            try
            {
                return await pipeline.ExecuteAsync(
                    async ct => await work(),
                    context.CancellationToken);
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex)
            {
                await context.LogWarningAsync(
                    $"Retry exhausted ({MaxAttempts} attempts) for " +
                    $"{action.GetType().Name}: {ex.Message}");

                if (Pipeline != null)
                {
                    var errorCtx = context.New(
                        new ErrorPayload(ex, originalInput, action));
                    return await Pipeline.ExecuteAsync(errorCtx);
                }
                throw;
            }
        }
    }
}
