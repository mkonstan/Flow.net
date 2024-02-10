using Flow.Policy;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Polly;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading.Tasks;

namespace Flow
{
    public abstract class PipelineAction : IPipelineAction
    {
        private static readonly SmartFormat.SmartFormatter Formatter = CreateDefaultFormater();
        private readonly IDictionary<Type, Func<IExecutionContext, IValue, Task<IValue>>> _handlers
            = new Dictionary<Type, Func<IExecutionContext, IValue, Task<IValue>>>();

        public PipelineAction() { Name = GetType().Name; }

        private string Name { get; }

        [JsonIgnore]
        private Guid Id { get; } = Guid.NewGuid();

        public IExecutionPolicy ExecutionPolicy { get; set; } = new DefaultPolicy();
        public IValueProvider InputProvider { get; set; } = new DefaultValueProvider();

        protected void SetTypeHandler<TIn>(Func<IExecutionContext, TIn, Task<IValue>> handler)
            where TIn : IValue
        { _handlers[typeof(TIn)] = async (context, input) => await handler(context, (TIn)input); }

        public async Task<IValue> ExecuteAsync(IExecutionContext context)
        {            
            var input = InputProvider.Get(context, this);
            var type = input.GetType();
            var policy = ExecutionPolicy.CreatePolicy(this, context, input);
            try
            {
                await context.LogInfoAsync($"{Name}:{Id} executing");
                var formatter = GetFormatter(type);
                var result = await policy.ExecuteAsync(async () => await formatter(context, input));
                await context.LogInfoAsync($"{Name}:{Id} compleated");
                return result;
            }
            catch (PipelineException ex)
            {
                await context.LogErrorAsync($"{Name}:{Id} Failed[{ new { State = this, Context = context, Payload = input, Exception = ex }.Serialize()}\nERROR:[{ex.Message}]");
                throw;
            }
            catch (Exception ex)
            {
                await context.LogErrorAsync($"{Name}:{Id} Failed[{ new { State = this, Context = context, Payload = input, Exception = ex }.Serialize()}\nERROR:[{ex.Message}]");
                throw new PipelineException(this, ex);
            }
        }

        protected virtual async Task<IValue> DefaultHandlerAsync(IExecutionContext context, IValue input)
        { return await Task.FromException<IValue>(new NotImplementedException()); }

        protected static string Format(
            string template,
            IExecutionContext context,
            IValue input,
            IPipelineAction action)
        {
            try
            {
                if (template == null) return template;
                var result = Formatter.Format(
                    template,
                    new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
                    { { "action", action }, { "session", context.Session.GetState() }, { "scope", context.Scope.GetState() }, { "input", input } });
                return result;
            }
            catch (Exception)
            {
                throw;
            }
        }

        private Func<IExecutionContext, IValue, Task<IValue>> GetFormatter(Type type)
        {
            if (!_handlers.Any()) return DefaultHandlerAsync;
            var actions = _handlers
                .Where(kv => kv.Key.IsAssignableFrom(type))
                .Select(kv => kv.Value);
            if (!actions.Any()) return DefaultHandlerAsync;
            return actions.SingleOrDefault() ?? DefaultHandlerAsync;
        }

        private static SmartFormat.SmartFormatter CreateDefaultFormater()
        {
            var formatter = SmartFormat.Smart.CreateDefaultSmartFormat();
            formatter.Settings.Parser.ConvertCharacterStringLiterals = false;
            return formatter;
        }

    }
}
