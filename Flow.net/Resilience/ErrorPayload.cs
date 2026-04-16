using System;

namespace Flow
{
    public sealed class ErrorPayload : IValueSource
    {
        public ErrorPayload(Exception exception, IValueSource originalInput, IPipelineAction failedAction)
        {
            Exception = exception ?? throw new ArgumentNullException(nameof(exception));
            OriginalInput = originalInput ?? throw new ArgumentNullException(nameof(originalInput));
            FailedAction = failedAction ?? throw new ArgumentNullException(nameof(failedAction));
        }

        public Exception Exception { get; }
        public IValueSource OriginalInput { get; }
        public IPipelineAction FailedAction { get; }
        public Type Type => typeof(ErrorPayload);
    }
}
