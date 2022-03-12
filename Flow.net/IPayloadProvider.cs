using System;
using System.Collections.Generic;
using System.Text;

namespace Flow
{
    public interface IPayloadProvider
    {
        IPayload GetPayload(IExecutionContext context, IPipelineAction action);
        T GetPayload<T>(IExecutionContext context, IPipelineAction action) where T : IPayload;
    }

    public class DefaultPayloadProvider : IPayloadProvider
    {
        public virtual IPayload GetPayload(IExecutionContext context, IPipelineAction action)
            => context.Result;

        public T GetPayload<T>(IExecutionContext context, IPipelineAction action) where T : IPayload
            => (T) GetPayload(context, action);
    }

    public class GetPayloadFromScope : DefaultPayloadProvider
    {
        public string Name { get; set; }

        public override IPayload GetPayload(IExecutionContext context, IPipelineAction action)
            => (IPayload)context.Scope[Name];
    }

    public class GetPayloadFromSession : DefaultPayloadProvider
    {
        public string Name { get; set; }

        public override IPayload GetPayload(IExecutionContext context, IPipelineAction action)
            => (IPayload)context.Session[Name];
    }
}
