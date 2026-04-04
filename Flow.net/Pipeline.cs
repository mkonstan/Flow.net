using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Flow
{
    public class Pipeline : PipelineAction, IPipeline
    {
        public IEnumerable<IPipelineAction> Actions { get; set; }

        protected override async Task<IValueSource> DefaultHandlerAsync(IExecutionContext context, IValueSource input)
        {
            if (Actions == null)
                throw new ActionConfigurationException(GetType().Name, "Actions must be set before execution.");

            foreach (var action in Actions)
            {
                input = await action.ExecuteAsync(context = context.New(input));
            }
            return input;
        }
    }
}
