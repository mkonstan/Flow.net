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
        private readonly IDictionary<Type, Func<IExecutionContext, IValueSource, Task<IValueSource>>> _handlers
            = new Dictionary<Type, Func<IExecutionContext, IValueSource, Task<IValueSource>>>();

        public PipelineAction() { Name = GetType().Name; }

        private string Name { get; }

        [JsonIgnore]
        private Guid Id { get; } = Guid.NewGuid();

        public IPayloadProvider PayloadProvider { get; set; } = new DefaultPayloadProvider();

        protected void SetTypeHandler<TIn>(Func<IExecutionContext, TIn, Task<IValueSource>> handler)
            where TIn : IValueSource
        { _handlers[typeof(TIn)] = async (context, input) => await handler(context, (TIn)input); }

        public async Task<IValueSource> ExecuteAsync(IExecutionContext context)
        {
            var input = PayloadProvider.GetPayload(context, this);
            var type = input.GetType();
            try
            {
                await context.LogInfoAsync($"{Name}:{Id} executing");
                var result = await GetFormatter(type)(context, input);
                await context.LogInfoAsync($"{Name}:{Id} completed");
                return result;
            }
            catch (Exception ex)
            {
                await context.LogErrorAsync($"{Name}:{Id} Failed[{ new { State = SanitizeForLogging(this), Context = context, Payload = input, Exception = ex }.Serialize()}\nERROR:[{ex.Message}]");
                throw;
            }
        }

        protected virtual Task<IValueSource> DefaultHandlerAsync(IExecutionContext context, IValueSource input)
            => Task.FromException<IValueSource>(
                new HandlerNotFoundException(GetType().Name, input.GetType().Name));

        protected static string Format(
            string template,
            IExecutionContext context,
            IValueSource input,
            IPipelineAction action)
        {
            if (template == null)
                throw new ActionConfigurationException(action.GetType().Name, "Template cannot be null. Ensure the action property is set before execution.");

            return Formatter.Format(
                template,
                new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
                { { "action", action }, { "session", context.Session.GetState() }, { "scope", context.Scope.GetState() }, { "input", input } });
        }

        private Func<IExecutionContext, IValueSource, Task<IValueSource>> GetFormatter(Type type)
        {
            if (!_handlers.Any()) return DefaultHandlerAsync;
            var match = _handlers
                .Where(kv => kv.Key.IsAssignableFrom(type))
                .OrderBy(kv => GetInheritanceDepth(type, kv.Key))
                .Select(kv => kv.Value)
                .FirstOrDefault();
            return match ?? DefaultHandlerAsync;
        }

        private static int GetInheritanceDepth(Type type, Type handlerType)
        {
            // Exact match is always best
            if (type == handlerType) return 0;

            // Class hierarchy: walk BaseType chain
            if (!handlerType.IsInterface)
            {
                int depth = 0;
                var current = type;
                while (current != null && current != handlerType)
                {
                    depth++;
                    current = current.BaseType;
                }
                return current == handlerType ? depth : int.MaxValue;
            }

            // Interface: find how far up the chain we first see it
            int level = 0;
            var t = type;
            while (t != null)
            {
                if (t.GetInterfaces().Contains(handlerType))
                {
                    // Check if this level directly declares it (vs inheriting it)
                    var parentInterfaces = t.BaseType?.GetInterfaces();
                    if (parentInterfaces == null || !parentInterfaces.Contains(handlerType))
                        return level + 1; // +1 so concrete type match at same level wins
                }
                level++;
                t = t.BaseType;
            }
            return int.MaxValue;
        }

        private static IDictionary<string, object> SanitizeForLogging(PipelineAction action)
        {
            var props = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
            foreach (var prop in action.GetType().GetProperties())
            {
                var name = prop.Name;
                try
                {
                    var value = prop.GetValue(action);
                    if (name.Contains("ConnectionString", StringComparison.OrdinalIgnoreCase) ||
                        name.Contains("Password", StringComparison.OrdinalIgnoreCase))
                    {
                        props[name] = "***MASKED***";
                    }
                    else
                    {
                        props[name] = value;
                    }
                }
                catch
                {
                    props[name] = "<error reading property>";
                }
            }
            return props;
        }

        private static SmartFormat.SmartFormatter CreateDefaultFormater()
        {
            var formatter = SmartFormat.Smart.CreateDefaultSmartFormat();
			formatter.Settings.Parser.ConvertCharacterStringLiterals = false;
            return formatter;
        }
    }
}
