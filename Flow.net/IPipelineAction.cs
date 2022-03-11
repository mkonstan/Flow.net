using System;
using System.Linq;
using System.Threading.Tasks;

namespace Flow
{
    public interface IPipelineActionProp
    {
        IPayload GetValue(IExecutionContext context, IPipeline action);
    }

    public interface IPipelineAction
    {
        IPayloadProvider PayloadProvider { get; set; }
        Task<IPayload> ExecuteAsync(IExecutionContext context);
    }
}
