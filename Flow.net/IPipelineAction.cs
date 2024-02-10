using System;
using System.Linq;
using System.Threading.Tasks;

namespace Flow
{
    public interface IPipelineAction
    {
        IValueProvider InputProvider { get; set; }
        Task<IValue> ExecuteAsync(IExecutionContext context);
    }
}
