using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Flow
{
    public class Pipeline : PipelineAction, IPipeline
    {
        public IEnumerable<IPipelineAction> Actions { get; set; }

        protected override async Task<IPayload> DefaultHandlerAsync(IExecutionContext context, IPayload input)
        {
            var newContext = context.New();
            var responce = input;
            foreach (var action in Actions)
            {
                responce = await action.ExecuteAsync(newContext, responce);
            }
            return responce;
        }
    }
}
