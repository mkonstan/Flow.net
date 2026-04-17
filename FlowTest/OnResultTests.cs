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
    #region OnResult Test Infrastructure

    class CaptureAction : PipelineAction
    {
        public bool WasCalled { get; private set; }
        public int CallCount { get; private set; }
        public IValueSource CapturedInput { get; private set; }
        public IExecutionContext CapturedContext { get; private set; }

        public CaptureAction()
        {
            SetTypeHandler<IValueSource>((context, input) =>
            {
                WasCalled = true;
                CallCount++;
                CapturedInput = input;
                CapturedContext = context;
                return Task.FromResult(input);
            });
        }
    }

    class SideEffectAction : PipelineAction
    {
        public string ScopeKeyToWrite { get; set; }
        public object ScopeValueToWrite { get; set; }
        public string SessionKeyToWrite { get; set; }
        public object SessionValueToWrite { get; set; }
        public string ScopeKeyToRead { get; set; }
        public string SessionKeyToRead { get; set; }
        public object ReadScopeValue { get; private set; }
        public object ReadSessionValue { get; private set; }
        public Exception ToThrow { get; set; }

        public SideEffectAction()
        {
            SetTypeHandler<IValueSource>((context, input) =>
            {
                if (ScopeKeyToRead != null)
                {
                    try { ReadScopeValue = context.Scope[ScopeKeyToRead]; } catch { }
                }
                if (SessionKeyToRead != null)
                {
                    try { ReadSessionValue = context.Session[SessionKeyToRead]; } catch { }
                }
                if (ScopeKeyToWrite != null)
                    context.Scope[ScopeKeyToWrite] = ScopeValueToWrite;
                if (SessionKeyToWrite != null)
                    context.Session[SessionKeyToWrite] = SessionValueToWrite;
                if (ToThrow != null)
                    throw ToThrow;
                return Task.FromResult(input);
            });
        }
    }

    class ReturnConstantAction : PipelineAction
    {
        public IValueSource Value { get; set; }

        public ReturnConstantAction()
        {
            SetTypeHandler<IValueSource>((context, input) => Task.FromResult(Value));
        }
    }

    class OceThrowingAction : PipelineAction
    {
        public OceThrowingAction()
        {
            SetTypeHandler<IValueSource>((context, input) =>
            {
                throw new OperationCanceledException();
            });
        }
    }

    class OrderedCaptureAction : PipelineAction
    {
        private readonly List<int> _shared;
        public int Order { get; set; }

        public OrderedCaptureAction(List<int> shared, int order)
        {
            _shared = shared;
            Order = order;
            SetTypeHandler<IValueSource>((context, input) =>
            {
                _shared.Add(Order);
                return Task.FromResult(input);
            });
        }
    }

    // Mutates a mutable reference value (List<string>) stored in Scope —
    // proves the shallow-state-copy contract of OnResult isolation.
    class ScopeListMutatorAction : PipelineAction
    {
        public string ScopeKey { get; set; }
        public string ItemToAdd { get; set; }

        public ScopeListMutatorAction()
        {
            SetTypeHandler<IValueSource>((context, input) =>
            {
                var list = (List<string>)context.Scope[ScopeKey];
                list.Add(ItemToAdd);
                return Task.FromResult(input);
            });
        }
    }

    class LogSnapshotAction : PipelineAction
    {
        public int InfosCountAtCall { get; private set; }
        public List<string> InfosSnapshot { get; private set; }
        private readonly ResilienceTestLogger _logger;

        public LogSnapshotAction(ResilienceTestLogger logger)
        {
            _logger = logger;
            SetTypeHandler<IValueSource>((context, input) =>
            {
                InfosCountAtCall = _logger.Infos.Count;
                InfosSnapshot = _logger.Infos.ToList();
                return Task.FromResult(input);
            });
        }
    }

    #endregion

    [TestClass]
    public class OnResultTests
    {
        private ResilienceTestLogger _logger;
        private ResilienceTestContext _context;

        [TestInitialize]
        public void Setup()
        {
            _logger = new ResilienceTestLogger();
            _context = new ResilienceTestContext(_logger);
        }

        // 1. L3 — null default preserves existing behavior
        [TestMethod]
        public async Task OnResult_Null_Default_Behavior_Unchanged()
        {
            var expected = new ValuePrimitive<string>("payload");
            var action = new ReturnConstantAction { Value = expected };
            var result = await action.ExecuteAsync(_context);
            Assert.AreSame(expected, result);
            Assert.IsFalse(_logger.Warnings.Any(w => w.Contains("OnResult")));
        }

        // 2. L4 — OnResult fires after primary work succeeds
        [TestMethod]
        public async Task OnResult_FiresAfter_PrimaryWork_Succeeds()
        {
            var expected = new ValuePrimitive<string>("payload");
            var capture = new CaptureAction();
            var action = new ReturnConstantAction
            {
                Value = expected,
                OnResult = PipelineBuilder.CreatePipeline(capture)
            };
            var result = await action.ExecuteAsync(_context);
            Assert.IsTrue(capture.WasCalled);
            Assert.AreSame(expected, result);
        }

        // 3. L4 — OnResult does not fire when action throws without ErrorHandler
        [TestMethod]
        public async Task OnResult_DoesNotFire_WhenActionThrows_WithoutErrorHandler()
        {
            var capture = new CaptureAction();
            var action = new ThrowingAction
            {
                OnResult = PipelineBuilder.CreatePipeline(capture)
            };
            await Assert.ThrowsExceptionAsync<InvalidOperationException>(
                () => action.ExecuteAsync(_context));
            Assert.IsFalse(capture.WasCalled);
        }

        // 4. L8 — ContinueHandler's NullResult recovery does NOT trigger OnResult
        [TestMethod]
        public async Task OnResult_DoesNotFire_WhenContinueHandler_Recovers()
        {
            var capture = new CaptureAction();
            var action = new ThrowingAction
            {
                ErrorHandler = new ContinueHandler(),
                OnResult = PipelineBuilder.CreatePipeline(capture)
            };
            var result = await action.ExecuteAsync(_context);
            Assert.IsInstanceOfType(result, typeof(NullResult));
            Assert.IsFalse(capture.WasCalled);
        }

        // 5. L8 — RetryHandler fallback pipeline's synthesized value does NOT trigger OnResult
        [TestMethod]
        public async Task OnResult_DoesNotFire_WhenRetryHandler_FallbackPipeline_Recovers()
        {
            var capture = new CaptureAction();
            var fallbackCapture = new FallbackCapture();
            var action = new ThrowingAction
            {
                ErrorHandler = new RetryHandler
                {
                    MaxAttempts = 2,
                    InitialBackoff = TimeSpan.FromMilliseconds(1),
                    BackoffMultiplier = 1.0,
                    Pipeline = PipelineBuilder.CreatePipeline(fallbackCapture)
                },
                OnResult = PipelineBuilder.CreatePipeline(capture)
            };
            await action.ExecuteAsync(_context);
            Assert.IsNotNull(fallbackCapture.CapturedPayload, "Fallback pipeline must have fired.");
            Assert.IsFalse(capture.WasCalled, "OnResult must NOT fire when value is handler-recovered.");
        }

        // 6. L4/L8 — Retry that eventually succeeds DOES trigger OnResult
        [TestMethod]
        public async Task OnResult_Fires_WhenRetryHandler_EventuallySucceeds()
        {
            var capture = new CaptureAction();
            var counting = new CountingAction
            {
                FailUntilAttempt = 2,
                ErrorHandler = new RetryHandler
                {
                    MaxAttempts = 3,
                    InitialBackoff = TimeSpan.FromMilliseconds(1),
                    BackoffMultiplier = 1.0
                },
                OnResult = PipelineBuilder.CreatePipeline(capture)
            };
            await counting.ExecuteAsync(_context);
            Assert.AreEqual(3, counting.CallCount);
            Assert.IsTrue(capture.WasCalled);
        }

        // 7. L5 — OnResult pipeline sees the action's result as input
        [TestMethod]
        public async Task OnResult_Pipeline_SeesActionResult_AsInput()
        {
            var actionResult = new ValuePrimitive<int>(42);
            var capture = new CaptureAction();
            var action = new ReturnConstantAction
            {
                Value = actionResult,
                OnResult = PipelineBuilder.CreatePipeline(capture)
            };
            await action.ExecuteAsync(_context);
            Assert.AreSame(actionResult, capture.CapturedInput);
        }

        // 8. L6 — OnResult pipeline's return value is discarded
        [TestMethod]
        public async Task OnResult_Pipeline_ReturnValue_Ignored()
        {
            var original = new ValuePrimitive<string>("original");
            var sideChannel = new ValuePrimitive<string>("sideChannel");
            var action = new ReturnConstantAction
            {
                Value = original,
                OnResult = new Pipeline
                {
                    Actions = new IPipelineAction[]
                    {
                        new ReturnConstantAction { Value = sideChannel }
                    }
                }
            };
            var result = await action.ExecuteAsync(_context);
            Assert.AreSame(original, result);
        }

        // 9. L7 — non-OCE exceptions from OnResult are swallowed and logged as warning
        [TestMethod]
        public async Task OnResult_Exception_Swallowed_LoggedAsWarning()
        {
            var action = new ReturnConstantAction
            {
                Value = new ValuePrimitive<string>("ok"),
                OnResult = new Pipeline
                {
                    Actions = new IPipelineAction[]
                    {
                        new SideEffectAction { ToThrow = new InvalidOperationException("boom") }
                    }
                }
            };
            await action.ExecuteAsync(_context);
            Assert.IsTrue(
                _logger.Warnings.Any(w => w.Contains("boom") && w.Contains("OnResult")),
                "Expected a warning containing 'boom' and 'OnResult'.");
        }

        // 10. L7/L12 — OnResult failures do not fail the action
        [TestMethod]
        public async Task OnResult_Exception_DoesNotFailAction()
        {
            var expected = new ValuePrimitive<string>("ok");
            var action = new ReturnConstantAction
            {
                Value = expected,
                OnResult = new Pipeline
                {
                    Actions = new IPipelineAction[]
                    {
                        new SideEffectAction { ToThrow = new InvalidOperationException("boom") }
                    }
                }
            };
            var result = await action.ExecuteAsync(_context);
            Assert.AreSame(expected, result);
        }

        // 11. L13 — OperationCanceledException from OnResult propagates
        [TestMethod]
        public async Task OnResult_OCE_Propagates()
        {
            var action = new ReturnConstantAction
            {
                Value = new ValuePrimitive<string>("ok"),
                OnResult = new Pipeline
                {
                    Actions = new IPipelineAction[] { new OceThrowingAction() }
                }
            };
            await Assert.ThrowsExceptionAsync<OperationCanceledException>(
                () => action.ExecuteAsync(_context));
            Assert.IsFalse(
                _logger.Warnings.Any(w => w.Contains("OnResult failed")),
                "OCE path must not log an OnResult-failed warning.");
        }

        // 12. L11 — Scope key reassignments in OnResult do not leak back to parent.
        // This proves SHALLOW isolation only (dictionary-level). See test 18 for the
        // explicit shallow-copy caveat on mutable reference values.
        [TestMethod]
        public async Task OnResult_IsolatedContext_ScopeKeyReassignment_DoesNotLeakBack()
        {
            _context.Scope["k"] = "parent";
            var side = new SideEffectAction
            {
                ScopeKeyToRead = "k",
                ScopeKeyToWrite = "k",
                ScopeValueToWrite = "child"
            };
            var action = new ReturnConstantAction
            {
                Value = new ValuePrimitive<string>("ok"),
                OnResult = PipelineBuilder.CreatePipeline(side)
            };
            await action.ExecuteAsync(_context);
            Assert.AreEqual("parent", _context.Scope["k"], "Parent Scope must not reflect child's write.");
            Assert.AreEqual("parent", side.ReadScopeValue, "Child must have observed the parent's value via clone.");
        }

        // 13. L11 — Session key reassignments in OnResult do not leak back to parent.
        // Same shallow-isolation caveat as test 12.
        [TestMethod]
        public async Task OnResult_IsolatedContext_SessionKeyReassignment_DoesNotLeakBack()
        {
            _context.Session["k"] = "parent";
            var side = new SideEffectAction
            {
                SessionKeyToRead = "k",
                SessionKeyToWrite = "k",
                SessionValueToWrite = "child"
            };
            var action = new ReturnConstantAction
            {
                Value = new ValuePrimitive<string>("ok"),
                OnResult = PipelineBuilder.CreatePipeline(side)
            };
            await action.ExecuteAsync(_context);
            Assert.AreEqual("parent", _context.Session["k"], "Parent Session must not reflect child's write.");
            Assert.AreEqual("parent", side.ReadSessionValue, "Child must have observed the parent's value via clone.");
        }

        // 14. L10 — OnResult can contain ForEach for fan-out
        [TestMethod]
        public async Task OnResult_CanContain_ForEach_ForFanOut()
        {
            var items = new PayloadCollection(new IValueSource[]
            {
                new ValuePrimitive<int>(1),
                new ValuePrimitive<int>(2),
                new ValuePrimitive<int>(3)
            });
            var capture = new CaptureAction();
            var action = new ReturnConstantAction
            {
                Value = items,
                OnResult = new Pipeline
                {
                    Actions = new IPipelineAction[]
                    {
                        new ForEach { Actions = new IPipelineAction[] { capture } }
                    }
                }
            };
            await action.ExecuteAsync(_context);
            Assert.AreEqual(3, capture.CallCount);
        }

        // 15. L9 — Nested OnResult composes
        [TestMethod]
        public async Task OnResult_NestedOnResult_Composes()
        {
            var capture = new CaptureAction();
            var innerAction = new ReturnConstantAction
            {
                Value = new ValuePrimitive<string>("inner"),
                OnResult = PipelineBuilder.CreatePipeline(capture)
            };
            var outerAction = new ReturnConstantAction
            {
                Value = new ValuePrimitive<string>("outer"),
                OnResult = PipelineBuilder.CreatePipeline(innerAction)
            };
            await outerAction.ExecuteAsync(_context);
            Assert.IsTrue(capture.WasCalled);
        }

        // 16. L12 — "completed" log fires before OnResult runs
        [TestMethod]
        public async Task OnResult_CompletionLog_FiresBefore_OnResult()
        {
            var snapshot = new LogSnapshotAction(_logger);
            var action = new ReturnConstantAction
            {
                Value = new ValuePrimitive<string>("ok"),
                OnResult = PipelineBuilder.CreatePipeline(snapshot)
            };
            await action.ExecuteAsync(_context);
            Assert.IsNotNull(snapshot.InfosSnapshot);
            Assert.IsTrue(
                snapshot.InfosSnapshot.Any(i => i.Contains("completed")),
                "At OnResult invocation time, a 'completed' log line must already exist.");
        }

        // 18. L11 HONEST-LIMIT — Shallow state copy: mutable reference VALUES
        // stored in Scope/Session remain shared with the parent. This is the
        // documented limitation of OnResult isolation — callers must store
        // immutable values in Scope/Session if they need full isolation.
        // This test EXISTS to keep the shallow-copy contract honest and aligned
        // with the comments on PipelineAction.CreateIsolatedContext and
        // ExecutionContext(IExecutionContext, IValueSource).
        [TestMethod]
        public async Task OnResult_SharedReferenceValue_MutationLeaksAcrossBoundary()
        {
            var sharedList = new List<string> { "parent-wrote-this" };
            _context.Scope["items"] = sharedList;

            var mutator = new ScopeListMutatorAction { ScopeKey = "items", ItemToAdd = "child-wrote-this" };
            var action = new ReturnConstantAction
            {
                Value = new ValuePrimitive<string>("ok"),
                OnResult = PipelineBuilder.CreatePipeline(mutator)
            };

            await action.ExecuteAsync(_context);

            // Parent's list DOES contain the child's mutation — proving shallow copy.
            // If this test ever STARTS failing (i.e., the mutation no longer leaks),
            // we've accidentally implemented deep isolation and the contract docs
            // must be updated to match. Do NOT "fix" this test by asserting the
            // mutation didn't leak — that would silently return to overclaiming.
            CollectionAssert.Contains(sharedList, "child-wrote-this",
                "Shallow state copy means mutable reference values ARE shared. " +
                "If this fails, isolation semantics changed — update the contract.");
        }

        // 17. Basic semantics — OnResult actions run in order
        [TestMethod]
        public async Task OnResult_MultipleActions_RunsInOrder()
        {
            var order = new List<int>();
            var action = new ReturnConstantAction
            {
                Value = new ValuePrimitive<string>("ok"),
                OnResult = new Pipeline
                {
                    Actions = new IPipelineAction[]
                    {
                        new OrderedCaptureAction(order, 1),
                        new OrderedCaptureAction(order, 2),
                        new OrderedCaptureAction(order, 3)
                    }
                }
            };
            await action.ExecuteAsync(_context);
            CollectionAssert.AreEqual(new[] { 1, 2, 3 }, order);
        }
    }
}
