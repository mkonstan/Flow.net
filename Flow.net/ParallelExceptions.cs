using System;

namespace Flow
{
    public class ParallelPipelineException : FlowException
    {
        public ParallelPipelineException(string message, AggregateException innerException)
            : base(message, innerException)
        { }

        public AggregateException AggregateException => (AggregateException)InnerException;
    }

    public class ParallelForEachException : FlowException
    {
        public ParallelForEachException(string message, AggregateException innerException)
            : base(message, innerException)
        { }

        public AggregateException AggregateException => (AggregateException)InnerException;
    }
}
