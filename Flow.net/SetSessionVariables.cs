using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading.Tasks;

namespace Flow
{
    [EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class SetStateVariables : PipelineAction
    {
        public IDictionary<string, object> Variables = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
        private readonly Action<IExecutionContext, KeyValuePair<string, object>> _assignment;

        protected SetStateVariables(Action<IExecutionContext, KeyValuePair<string, object>> assignment)
        {
            _assignment = assignment;
        }

        public void AddStateVariable(string name, object value) => Variables[name] = value;
        protected sealed override async Task<IPayload> DefaultHandlerAsync(IExecutionContext context, IPayload input)
        {
            foreach (var element in Variables)
            {
                _assignment(context, element);
            }
            return await Task.FromResult(input);
        }


    }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class StoreInState: PipelineAction
    {
        private readonly Action<IExecutionContext, string, IPayload> _assignment;
        protected StoreInState(Action<IExecutionContext, string, IPayload> assignment)
        {
            _assignment = assignment;
        }

        public string Name { get; set; }

        protected sealed override async Task<IPayload> DefaultHandlerAsync(IExecutionContext context, IPayload payload)
        {
            _assignment(context, Name, payload);
            return await Task.FromResult(payload);
        }
    }

    public class StoreInScope : StoreInState
    {
        public StoreInScope()
            :base((context, name, payload) => context.Scope[name] = payload)
        { }
    }

    public class StoreInSession : StoreInState
    {
        public StoreInSession()
            : base((context, name, payload) => context.Session[name] = payload)
        { }
    }


    public class SetScopeVariables : SetStateVariables
    {
        public SetScopeVariables()
            :base((context, element) => context.Scope[element.Key] = element.Value)
        { }
    }

    public class SetSessionVariables : SetStateVariables
    {
        public SetSessionVariables()
            : base((context, element) => context.Session[element.Key] = element.Value)
        { }
    }
}
