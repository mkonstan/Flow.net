using System;
using System.Threading.Tasks;

namespace Flow
{
    public interface IPipelineBuilder : IPipelineActionBuilder
    {
        Task<IPayload> ExecuteAsync();
        IPipelineBuilder ContinueWith<T>(Action<T> body) where T : IPipelineAction, new();
        IPipeline Create();
    }
}
