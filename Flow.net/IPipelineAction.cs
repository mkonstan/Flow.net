using System;
using System.Linq;
using System.Threading.Tasks;

namespace Flow
{
    public interface IPipelineAction
    {
        IPayloadProvider PayloadProvider { get; set; }
        Task<IValueSource> ExecuteAsync(IExecutionContext context);
    }
}
