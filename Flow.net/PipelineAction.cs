using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Flow
{
    public abstract class PipelineAction : IPipelineAction
    {
        private static readonly SmartFormat.SmartFormatter Formatter = CreateDefaultFormater();
        private readonly IDictionary<Type, Func<IExecutionContext, IPayload, Task<IPayload>>> _handlers
            = new Dictionary<Type, Func<IExecutionContext, IPayload, Task<IPayload>>>();

        public PipelineAction() { Name = GetType().Name; }

        private string Name { get; }

        [JsonIgnore]
        private Guid Id { get; } = Guid.NewGuid();

        protected void SetTypeHandler<TIn>(Func<IExecutionContext, TIn, Task<IPayload>> handler)
            where TIn : IPayload
        { _handlers[typeof(TIn)] = async (context, input) => await handler(context, (TIn)input); }

        public async Task<IPayload> ExecuteAsync(IExecutionContext context, IPayload input)
        {
            var type = input.GetType();
            try
            {
                await context.LogInfoAsync($"{Name}:{Id} executing");
                var result = await GetFormatter(type)(context, input);
                await context.LogInfoAsync($"{Name}:{Id} compleated");
                return result;
            }
            catch (Exception ex)
            {
                await context.LogErrorAsync($"{Name}:{Id} Failed[{ new { State = this, Context = context, Payload = input, Exception = ex }.Serialize()}\nERROR:[{ex.Message}]");
                throw;
            }
        }

        protected virtual async Task<IPayload> DefaultHandlerAsync(IExecutionContext context, IPayload input)
        { return await Task.FromException<IPayload>(new NotImplementedException()); }

        protected static string Format(
            string template,
            IExecutionContext context,
            IPayload input,
            IPipelineAction action)
        {
            try
            {
                if (template == null) return template;
                var state = context.GetState();
                var result = Formatter.Format(
                    template,
                    new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
                    { { "scope", action }, { "input", input }, { "state", state } });
                return result;
            }
            catch (Exception)
            {
                throw;
            }
        }

        private Func<IExecutionContext, IPayload, Task<IPayload>> GetFormatter(Type type)
        {
            if (!_handlers.Any()) return DefaultHandlerAsync;
            var actions = _handlers
                .Where(kv => kv.Key.IsAssignableFrom(type))
                .Select(kv => kv.Value);
            if(!actions.Any()) return DefaultHandlerAsync;
            return actions.SingleOrDefault() ?? DefaultHandlerAsync;
        }

        private static SmartFormat.SmartFormatter CreateDefaultFormater()
        {
            var formatter = SmartFormat.Smart.CreateDefaultSmartFormat();
            formatter.Settings.ConvertCharacterStringLiterals = false;
            return formatter;
        }
    }
}
