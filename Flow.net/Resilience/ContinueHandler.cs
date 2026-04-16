using System;
using System.Threading.Tasks;

namespace Flow
{
    public sealed class ContinueHandler : IErrorHandler
    {
        public IPipeline Pipeline { get; set; }

        public async Task<IValueSource> HandledActionAsync(
            IExecutionContext context, IPipelineAction action, IValueSource originalInput, Func<Task<IValueSource>> work)
        {
            try
            {
                return await work();
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex)
            {
                await context.LogWarningAsync(
                    $"Continuing past failure of {action.GetType().Name}: {ex.Message}");

                if (Pipeline != null)
                {
                    var errorCtx = context.New(
                        new ErrorPayload(ex, originalInput, action));
                    return await Pipeline.ExecuteAsync(errorCtx);
                }
                return NullResult.Instance;
            }
        }
    }
}
