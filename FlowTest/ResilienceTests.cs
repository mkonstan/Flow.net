using Flow;
using Flow.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace FlowTest
{
    #region Resilience Test Infrastructure

    class ResilienceTestContext : IExecutionContext
    {
        private readonly ILogger _logger;
        private IValueSource _result = NullResult.Instance;

        public ResilienceTestContext(ILogger logger, CancellationToken cancellationToken = default)
        {
            _logger = logger;
            CancellationToken = cancellationToken;
        }

        public IState Scope { get; } = new ResilienceDictState();
        public IState Session { get; } = new ResilienceDictState();
        public IValueSource Result => _result;
        public CancellationToken CancellationToken { get; }

        public Task LogErrorAsync(string message) => _logger.LogErrorAsync(message);
        public Task LogInfoAsync(string message) => _logger.LogInfoAsync(message);
        public Task LogWarningAsync(string message) => _logger.LogWarningAsync(message);

        public IExecutionContext New() => new ResilienceTestContext(_logger, CancellationToken);
        public IExecutionContext New(IValueSource result)
        {
            var ctx = new ResilienceTestContext(_logger, CancellationToken);
            ctx._result = result;
            return ctx;
        }

        class ResilienceDictState : IState
        {
            private readonly Dictionary<string, object> _state = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
            public object this[string name] { get => _state[name]; set => _state[name] = value; }
            public IEnumerator<KeyValuePair<string, object>> GetEnumerator() => _state.GetEnumerator();
            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
            public IDictionary<string, object> GetState() => new Dictionary<string, object>(_state, StringComparer.OrdinalIgnoreCase);
        }
    }

    class ThrowingAction : PipelineAction
    {
        public int CallCount { get; private set; }
        public string ErrorMessage { get; set; } = "Test failure";

        public ThrowingAction()
        {
            SetTypeHandler<IValueSource>((context, input) =>
            {
                CallCount++;
                throw new InvalidOperationException(ErrorMessage);
            });
        }
    }

    class CountingAction : PipelineAction
    {
        public int CallCount { get; private set; }
        public int FailUntilAttempt { get; set; } = 0;
        public string ErrorMessage { get; set; } = "Transient failure";

        public CountingAction()
        {
            SetTypeHandler<IValueSource>((context, input) =>
            {
                CallCount++;
                if (CallCount <= FailUntilAttempt)
                    throw new InvalidOperationException(ErrorMessage);
                return Task.FromResult(input);
            });
        }
    }

    class CancellationThrowingAction : PipelineAction
    {
        public CancellationThrowingAction()
        {
            SetTypeHandler<IValueSource>((context, input) =>
            {
                throw new OperationCanceledException();
            });
        }
    }

    class FallbackCapture : PipelineAction
    {
        public ErrorPayload CapturedPayload { get; private set; }

        public FallbackCapture()
        {
            SetTypeHandler<ErrorPayload>((context, input) =>
            {
                CapturedPayload = input;
                return Task.FromResult<IValueSource>(NullResult.Instance);
            });
        }
    }

    class ResilienceTestLogger : ILogger
    {
        public List<string> Errors { get; } = new List<string>();
        public List<string> Warnings { get; } = new List<string>();
        public List<string> Infos { get; } = new List<string>();

        public Task LogErrorAsync(string message) { Errors.Add(message); return Task.CompletedTask; }
        public Task LogInfoAsync(string message) { Infos.Add(message); return Task.CompletedTask; }
        public Task LogWarningAsync(string message) { Warnings.Add(message); return Task.CompletedTask; }
    }

    #endregion

    [TestClass]
    public class ResilienceTests
    {
        private ResilienceTestLogger _logger;
        private ResilienceTestContext _context;

        [TestInitialize]
        public void Setup()
        {
            _logger = new ResilienceTestLogger();
            _context = new ResilienceTestContext(_logger);
        }

        // 1. Default behavior unchanged when ErrorHandler == null
        [TestMethod]
        public async Task DefaultBehavior_NoHandler_ExceptionPropagates()
        {
            var action = new ThrowingAction();
            await Assert.ThrowsExceptionAsync<InvalidOperationException>(
                () => action.ExecuteAsync(_context));
        }

        // 2. Today's error logging preserved when ErrorHandler == null
        [TestMethod]
        public async Task DefaultBehavior_NoHandler_ErrorIsLogged()
        {
            var action = new ThrowingAction();
            try { await action.ExecuteAsync(_context); } catch { }
            Assert.IsTrue(_logger.Errors.Any(e => e.Contains("Failed[")));
        }

        // 3. ContinueHandler returns NullResult on failure
        [TestMethod]
        public async Task ContinueHandler_ReturnsNullResult_OnFailure()
        {
            var action = new ThrowingAction { ErrorHandler = new ContinueHandler() };
            var result = await action.ExecuteAsync(_context);
            Assert.IsInstanceOfType(result, typeof(NullResult));
        }

        // 4. ContinueHandler runs fallback Pipeline when set
        [TestMethod]
        public async Task ContinueHandler_RunsFallbackPipeline_WhenSet()
        {
            var capture = new FallbackCapture();
            var action = new ThrowingAction
            {
                ErrorHandler = new ContinueHandler
                {
                    Pipeline = PipelineBuilder.CreatePipeline(capture)
                }
            };
            var result = await action.ExecuteAsync(_context);
            Assert.IsNotNull(capture.CapturedPayload);
            Assert.IsInstanceOfType(capture.CapturedPayload.Exception, typeof(InvalidOperationException));
        }

        // 5. RetryHandler retries N times on persistent failure
        [TestMethod]
        public async Task RetryHandler_RetriesNTimes_OnPersistentFailure()
        {
            var action = new ThrowingAction
            {
                ErrorHandler = new RetryHandler
                {
                    MaxAttempts = 3,
                    InitialBackoff = TimeSpan.FromMilliseconds(1),
                    BackoffMultiplier = 1.0
                }
            };
            await Assert.ThrowsExceptionAsync<InvalidOperationException>(
                () => action.ExecuteAsync(_context));
            Assert.AreEqual(3, action.CallCount);
        }

        // 6. RetryHandler succeeds on second attempt
        [TestMethod]
        public async Task RetryHandler_SucceedsOnSecondAttempt()
        {
            var counting = new CountingAction
            {
                FailUntilAttempt = 1,
                ErrorHandler = new RetryHandler
                {
                    MaxAttempts = 3,
                    InitialBackoff = TimeSpan.FromMilliseconds(1),
                    BackoffMultiplier = 1.0
                }
            };
            var result = await counting.ExecuteAsync(_context);
            Assert.AreEqual(2, counting.CallCount);
        }

        // 7. RetryHandler exhaustion rethrows when no fallback
        [TestMethod]
        public async Task RetryHandler_Exhaustion_Rethrows_WhenNoFallback()
        {
            var action = new ThrowingAction
            {
                ErrorHandler = new RetryHandler
                {
                    MaxAttempts = 2,
                    InitialBackoff = TimeSpan.FromMilliseconds(1),
                    BackoffMultiplier = 1.0
                }
            };
            var ex = await Assert.ThrowsExceptionAsync<InvalidOperationException>(
                () => action.ExecuteAsync(_context));
            Assert.AreEqual("Test failure", ex.Message);
        }

        // 8. RetryHandler exhaustion runs fallback when set
        [TestMethod]
        public async Task RetryHandler_Exhaustion_RunsFallback_WhenSet()
        {
            var capture = new FallbackCapture();
            var action = new ThrowingAction
            {
                ErrorHandler = new RetryHandler
                {
                    MaxAttempts = 2,
                    InitialBackoff = TimeSpan.FromMilliseconds(1),
                    BackoffMultiplier = 1.0,
                    Pipeline = PipelineBuilder.CreatePipeline(capture)
                }
            };
            await action.ExecuteAsync(_context);
            Assert.IsNotNull(capture.CapturedPayload);
            Assert.AreEqual("Test failure", capture.CapturedPayload.Exception.Message);
        }

        // 9. RetryHandler honors ShouldRetry predicate
        [TestMethod]
        public async Task RetryHandler_HonorsShouldRetryPredicate()
        {
            var action = new ThrowingAction
            {
                ErrorHandler = new RetryHandler
                {
                    MaxAttempts = 5,
                    InitialBackoff = TimeSpan.FromMilliseconds(1),
                    BackoffMultiplier = 1.0,
                    ShouldRetry = ex => false
                }
            };
            await Assert.ThrowsExceptionAsync<InvalidOperationException>(
                () => action.ExecuteAsync(_context));
            Assert.AreEqual(1, action.CallCount);
        }

        // 10. RetryHandler honors cancellation mid-delay
        [TestMethod]
        public async Task RetryHandler_HonorsCancellation_MidDelay()
        {
            var cts = new CancellationTokenSource();
            var ctx = new ResilienceTestContext(_logger, cts.Token);
            var action = new ThrowingAction
            {
                ErrorHandler = new RetryHandler
                {
                    MaxAttempts = 10,
                    InitialBackoff = TimeSpan.FromSeconds(30),
                    BackoffMultiplier = 1.0
                }
            };
            cts.CancelAfter(TimeSpan.FromMilliseconds(100));
            try
            {
                await action.ExecuteAsync(ctx);
                Assert.Fail("Expected OperationCanceledException");
            }
            catch (OperationCanceledException)
            {
                // TaskCanceledException inherits OperationCanceledException — both are valid
            }
        }

        // 11. RetryHandler rethrows OperationCanceledException from work — no retry
        [TestMethod]
        public async Task RetryHandler_RethrowsOperationCanceledException_FromWork_NoRetry()
        {
            var action = new CancellationThrowingAction
            {
                ErrorHandler = new RetryHandler
                {
                    MaxAttempts = 5,
                    InitialBackoff = TimeSpan.FromMilliseconds(1)
                }
            };
            await Assert.ThrowsExceptionAsync<OperationCanceledException>(
                () => action.ExecuteAsync(_context));
        }

        // 12. RetryHandler does NOT invoke fallback Pipeline on cancellation
        [TestMethod]
        public async Task RetryHandler_DoesNotInvokeFallbackPipeline_OnCancellation()
        {
            var capture = new FallbackCapture();
            var action = new CancellationThrowingAction
            {
                ErrorHandler = new RetryHandler
                {
                    MaxAttempts = 3,
                    InitialBackoff = TimeSpan.FromMilliseconds(1),
                    Pipeline = PipelineBuilder.CreatePipeline(capture)
                }
            };
            await Assert.ThrowsExceptionAsync<OperationCanceledException>(
                () => action.ExecuteAsync(_context));
            Assert.IsNull(capture.CapturedPayload);
        }

        // 13. ContinueHandler rethrows OperationCanceledException
        [TestMethod]
        public async Task ContinueHandler_RethrowsOperationCanceledException()
        {
            var action = new CancellationThrowingAction
            {
                ErrorHandler = new ContinueHandler()
            };
            await Assert.ThrowsExceptionAsync<OperationCanceledException>(
                () => action.ExecuteAsync(_context));
        }

        // 14. ContinueHandler does NOT invoke fallback Pipeline on cancellation
        [TestMethod]
        public async Task ContinueHandler_DoesNotInvokeFallbackPipeline_OnCancellation()
        {
            var capture = new FallbackCapture();
            var action = new CancellationThrowingAction
            {
                ErrorHandler = new ContinueHandler
                {
                    Pipeline = PipelineBuilder.CreatePipeline(capture)
                }
            };
            await Assert.ThrowsExceptionAsync<OperationCanceledException>(
                () => action.ExecuteAsync(_context));
            Assert.IsNull(capture.CapturedPayload);
        }

        // 15. Custom IErrorHandler implementation works
        [TestMethod]
        public async Task CustomHandler_InterfaceContract_Works()
        {
            var customHandler = new TestCustomHandler();
            var action = new ThrowingAction { ErrorHandler = customHandler };
            var result = await action.ExecuteAsync(_context);
            Assert.IsInstanceOfType(result, typeof(NullResult));
            Assert.IsTrue(customHandler.WasCalled);
        }

        // 16. ParallelForEach with per-item handler isolates failures
        [TestMethod]
        public async Task ParallelForEach_PerItemHandler_IsolatesFailures()
        {
            var items = new PayloadCollection(new IValueSource[]
            {
                new ValuePrimitive<string>("a"),
                new ValuePrimitive<string>("b")
            });
            var forEach = new ParallelForEach
            {
                Actions = new IPipelineAction[]
                {
                    new ConditionalThrowAction
                    {
                        ThrowOnValue = "a",
                        ErrorHandler = new ContinueHandler()
                    }
                }
            };
            var ctx = _context.New(items);
            var result = await forEach.ExecuteAsync(ctx);
            Assert.IsInstanceOfType(result, typeof(PayloadCollection));
            var results = ((PayloadCollection)result).ToArray();
            Assert.AreEqual(2, results.Length);
            Assert.IsInstanceOfType(results[0], typeof(NullResult));
            Assert.IsInstanceOfType(results[1], typeof(ValuePrimitive<string>));
        }

        // 17. ParallelForEach two-layer recovery — outer handler catches what inner didn't handle
        [TestMethod]
        public async Task ParallelForEach_TwoLayerRecovery()
        {
            var items = new PayloadCollection(new IValueSource[]
            {
                new ValuePrimitive<string>("fail"),
                new ValuePrimitive<string>("fail")
            });
            var outerCapture = new FallbackCapture();
            var forEach = new ParallelForEach
            {
                Actions = new IPipelineAction[]
                {
                    // No inner ErrorHandler — per-item exceptions bubble to the composite
                    new ConditionalThrowAction { ThrowOnValue = "fail" }
                },
                ErrorHandler = new ContinueHandler
                {
                    Pipeline = PipelineBuilder.CreatePipeline(outerCapture)
                }
            };
            var ctx = _context.New(items);
            var result = await forEach.ExecuteAsync(ctx);
            // Outer ContinueHandler catches aggregated parallel failure, runs fallback pipeline,
            // which captures the ErrorPayload via outerCapture.
            Assert.IsNotNull(outerCapture.CapturedPayload, "Outer fallback pipeline must have fired.");
            Assert.IsInstanceOfType(result, typeof(NullResult));
        }

        // 18. ErrorPayload carries all fields
        [TestMethod]
        public void ErrorPayload_CarriesAllFields()
        {
            var ex = new InvalidOperationException("boom");
            var input = new ValuePrimitive<string>("test");
            var action = new ThrowingAction();
            var payload = new ErrorPayload(ex, input, action);
            Assert.AreSame(ex, payload.Exception);
            Assert.AreSame(input, payload.OriginalInput);
            Assert.AreSame(action, payload.FailedAction);
            Assert.AreEqual(typeof(ErrorPayload), payload.Type);
        }

        // 19. Handler can read action.GetType().Name for logging
        [TestMethod]
        public async Task Handler_CanReadActionTypeName_ForLogging()
        {
            var action = new ThrowingAction { ErrorHandler = new ContinueHandler() };
            await action.ExecuteAsync(_context);
            Assert.IsTrue(_logger.Warnings.Any(w => w.Contains("ThrowingAction")));
        }
    }

    #region Additional Resilience Test Helpers

    class TestCustomHandler : IErrorHandler
    {
        public bool WasCalled { get; private set; }
        public IPipeline Pipeline { get; set; }

        public async Task<IValueSource> HandledActionAsync(
            IExecutionContext context, IPipelineAction action, IValueSource originalInput, Func<Task<IValueSource>> work)
        {
            try
            {
                return await work();
            }
            catch
            {
                WasCalled = true;
                return NullResult.Instance;
            }
        }
    }

    class ConditionalThrowAction : PipelineAction
    {
        public string ThrowOnValue { get; set; }

        public ConditionalThrowAction()
        {
            SetTypeHandler<IValueSource>((context, input) =>
            {
                if (input is ValuePrimitive<string> str && str.Value == ThrowOnValue)
                    throw new InvalidOperationException($"Failed on {ThrowOnValue}");
                return Task.FromResult(input);
            });
        }
    }

    #endregion
}
