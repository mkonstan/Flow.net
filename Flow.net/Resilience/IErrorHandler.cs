using System;
using System.Threading.Tasks;

namespace Flow
{
    public interface IErrorHandler
    {
        IPipeline Pipeline { get; set; }

        Task<IValueSource> HandledActionAsync(
            IExecutionContext context,
            IPipelineAction action,
            IValueSource originalInput,
            Func<Task<IValueSource>> work);
    }
}
