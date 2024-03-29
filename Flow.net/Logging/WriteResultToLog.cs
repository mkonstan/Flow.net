﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Flow.Logging
{
    public class LogResult : PipelineAction
    {
        protected override async Task<IPayload> DefaultHandlerAsync(IExecutionContext context, IPayload input)
        {
            await context.LogInfoAsync(input.Serialize(true));
            return input;
        }
    }
    public class LogContext : PipelineAction
    {
        protected override async Task<IPayload> DefaultHandlerAsync(IExecutionContext context, IPayload input)
        {
            await context.LogInfoAsync(context.Serialize(true));
            return input;
        }
    }
}
