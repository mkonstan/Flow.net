using System;
using System.Collections.Generic;
using System.Text;

namespace Flow
{

	public class DefaultValueProvider : IValueProvider
    {
        public virtual IValue Get(IExecutionContext context, IPipelineAction action)
            => context.Result;

        public T Get<T>(IExecutionContext context, IPipelineAction action) where T : IValue
            => (T) Get(context, action);
    }

    public class GetValueFromScope : DefaultValueProvider
    {
        public string Name { get; set; }

        public override IValue Get(IExecutionContext context, IPipelineAction action)
            => (IValue)context.Scope[Name];
    }

    public class GetValueFromSession : DefaultValueProvider
    {
        public string Name { get; set; }

        public override IValue Get(IExecutionContext context, IPipelineAction action)
            => (IValue)context.Session[Name];
    }
}
