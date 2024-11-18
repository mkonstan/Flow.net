using System;
using System.Collections.Generic;
using System.Text;

namespace Flow
{
    public interface IPayloadProvider
    {
        IValueSource GetPayload(IExecutionContext context, IPipelineAction action);
        T GetPayload<T>(IExecutionContext context, IPipelineAction action) where T : IValueSource;
    }

    public class DefaultPayloadProvider : IPayloadProvider
    {
        public virtual IValueSource GetPayload(IExecutionContext context, IPipelineAction action)
            => context.Result;

        public T GetPayload<T>(IExecutionContext context, IPipelineAction action) where T : IValueSource
            => (T) GetPayload(context, action);
    }

    public class GetPayloadFromScope : DefaultPayloadProvider
    {
        public string Name { get; set; }

        public override IValueSource GetPayload(IExecutionContext context, IPipelineAction action)
            => (IValueSource)context.Scope[Name];
    }

    public class GetPayloadFromSession : DefaultPayloadProvider
    {
        public string Name { get; set; }

        public override IValueSource GetPayload(IExecutionContext context, IPipelineAction action)
            => (IValueSource)context.Session[Name];
    }
}
