using System;
using System.Collections.Generic;
using System.Linq;

namespace Flow
{
    public interface IPipeline : IPipelineAction
    {
        IEnumerable<IPipelineAction> Actions { get; set; }
    }
}
