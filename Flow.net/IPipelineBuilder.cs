using System;
using System.Threading;
using System.Threading.Tasks;

namespace Flow
{
    public interface IPipelineBuilder
    {
        Task<T> ExecuteAsync<T>(CancellationToken cancellationToken = default) where T : IValueSource;
        Task<IValueSource> ExecuteAsync(CancellationToken cancellationToken = default);
        IPipelineBuilder ContinueWith<T>(Action<T> body) where T : IPipelineAction, new();
        IPipelineBuilder ContinueWith<T>() where T : IPipelineAction, new();
        IPipeline Create();
    }
}
