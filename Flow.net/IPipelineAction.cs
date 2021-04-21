using System;
using System.Linq;
using System.Threading.Tasks;

namespace Flow
{
    public interface IPipelineAction
    {
        Task<IPayload> ExecuteAsync(IExecutionContext context, IPayload input);
    }
}
