using System;
using System.Threading.Tasks;

namespace Flow
{
    public interface IPipelineBuilder
    {
        Task<T> ExecuteAsync<T>() where T : IPayload;
        Task<IPayload> ExecuteAsync();
        IPipelineBuilder ContinueWith<T>(Action<T> body) where T : IPipelineAction, new();
        IPipelineBuilder ContinueWith<T>() where T : IPipelineAction, new();
        IPipeline Create();
    }
}
