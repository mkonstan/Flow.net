using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Newtonsoft.Json;
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
        public IErrorHandler ErrorHandler { get; set; }
        public IPipeline OnResult { get; set; }

        protected void SetTypeHandler<TIn>(Func<IExecutionContext, TIn, Task<IValueSource>> handler)
            where TIn : IValueSource
        { _handlers[typeof(TIn)] = async (context, input) => await handler(context, (TIn)input); }

        public async Task<IValueSource> ExecuteAsync(IExecutionContext context)
        {
            var input = PayloadProvider.GetPayload(context, this);
            bool workSucceeded = false;   // L8 — captured by the work closure; set only on real primary-work success

            Func<Task<IValueSource>> work = async () =>
            {
                await context.LogInfoAsync($"{Name}:{Id} executing");
                var result = await GetFormatter(input.GetType())(context, input);
                await context.LogInfoAsync($"{Name}:{Id} completed");
                workSucceeded = true;
                return result;
            };

            IValueSource actionResult;
            if (ErrorHandler == null)
            {
                try
                {
                    actionResult = await work();
                }
                catch (Exception ex)
                {
                    await context.LogErrorAsync($"{Name}:{Id} Failed[{ new { State = SanitizeForLogging(this), Context = context, Payload = input, Exception = ex }.Serialize()}\nERROR:[{ex.Message}]");
                    throw;
                }
            }
            else
            {
                actionResult = await ErrorHandler.HandledActionAsync(context, this, input, work);
            }

            // L4/L8 — OnResult fires only when primary work actually succeeded.
            // Handler-recovered values (ContinueHandler NullResult, RetryHandler fallback pipeline output) do NOT trigger it.
            if (OnResult != null && workSucceeded)
            {
                try
                {
                    var isolatedCtx = CreateIsolatedContext(context, actionResult);
                    await OnResult.ExecuteAsync(isolatedCtx);
                }
                catch (OperationCanceledException) { throw; }   // L13
                catch (Exception ex)
                {
                    await context.LogWarningAsync($"{Name}:{Id} OnResult failed: {ex.Message}");
                    // L7 — side-channel failures are non-fatal
                }
            }

            return actionResult;   // L6 — always the action's own result
        }

        // L11 — Shallow state copy. Produces a context whose Scope and Session DICTIONARIES
        // are fresh copies of the parent's, so key reassignments (`scope["k"] = v`) inside
        // the OnResult pipeline do not leak back to the main flow. Values stored in those
        // dictionaries are NOT deep-cloned: if you put a mutable reference (List, mutable
        // DTO, etc.) in Scope/Session, in-place mutation via that reference DOES leak.
        // Callers that need full isolation should put immutable values in Scope/Session.
        private static IExecutionContext CreateIsolatedContext(IExecutionContext parent, IValueSource payload)
            => new ExecutionContext(parent, payload);

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
                    if (name.IndexOf("ConnectionString", StringComparison.OrdinalIgnoreCase) >= 0 ||
                        name.IndexOf("Password", StringComparison.OrdinalIgnoreCase) >= 0)
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
