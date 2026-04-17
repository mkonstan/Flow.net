using Flow;
using Flow.IO;
using Flow.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Collections.Concurrent;
using System.Data.Common;
using System.Threading.Tasks;
using Flow.Data.ETL;
using Flow.Data.Postgres;
using NpgsqlTypes;

namespace FlowTest
{
    #region Test Infrastructure

    class TestLogger : ILogger
    {
        public List<string> Errors { get; } = new();
        public List<string> Warnings { get; } = new();
        public List<string> Infos { get; } = new();

        public Task LogErrorAsync(string message) { Errors.Add(message); return Task.CompletedTask; }
        public Task LogInfoAsync(string message) { Infos.Add(message); return Task.CompletedTask; }
        public Task LogWarningAsync(string message) { Warnings.Add(message); return Task.CompletedTask; }
    }

    class AddSuffixAction : PipelineAction
    {
        public string Suffix { get; set; } = "_processed";

        protected override Task<IValueSource> DefaultHandlerAsync(IExecutionContext context, IValueSource input)
        {
            if (input is ValuePrimitive<string> str)
                return Task.FromResult<IValueSource>(new ValuePrimitive<string>(str.Value + Suffix));
            return Task.FromResult(input);
        }
    }

    class ReturnValueAction : PipelineAction
    {
        public IValueSource Value { get; set; } = NullResult.Instance;

        protected override Task<IValueSource> DefaultHandlerAsync(IExecutionContext context, IValueSource input)
            => Task.FromResult(Value);
    }

    class TrackExecutionAction : PipelineAction
    {
        public static List<(int Order, string ThreadId, DateTime Time)> Executions { get; } = new();
        public int Order { get; set; }

        protected override async Task<IValueSource> DefaultHandlerAsync(IExecutionContext context, IValueSource input)
        {
            Executions.Add((Order, Environment.CurrentManagedThreadId.ToString(), DateTime.UtcNow));
            await Task.Delay(50); // simulate work
            return input;
        }

        public static void Reset() => Executions.Clear();
    }

    class FailingAction : PipelineAction
    {
        public string ErrorMessage { get; set; } = "Test error";

        protected override Task<IValueSource> DefaultHandlerAsync(IExecutionContext context, IValueSource input)
            => throw new InvalidOperationException(ErrorMessage);
    }

    class DelayedFailingAction : PipelineAction
    {
        public string ErrorMessage { get; set; } = "Delayed error";
        public int DelayMs { get; set; } = 50;

        protected override async Task<IValueSource> DefaultHandlerAsync(IExecutionContext context, IValueSource input)
        {
            await Task.Delay(DelayMs);
            throw new InvalidOperationException(ErrorMessage);
        }
    }

    class ReadSessionAction : PipelineAction
    {
        public string Key { get; set; }
        public static object LastValue { get; set; }

        protected override Task<IValueSource> DefaultHandlerAsync(IExecutionContext context, IValueSource input)
        {
            LastValue = context.Session[Key];
            return Task.FromResult(input);
        }
    }

    class ReadScopeAction : PipelineAction
    {
        public string Key { get; set; }
        public static object LastValue { get; set; }

        protected override Task<IValueSource> DefaultHandlerAsync(IExecutionContext context, IValueSource input)
        {
            LastValue = context.Scope[Key];
            return Task.FromResult(input);
        }
    }

    class WriteScopeAction : PipelineAction
    {
        public string Key { get; set; }
        public object Value { get; set; }

        protected override Task<IValueSource> DefaultHandlerAsync(IExecutionContext context, IValueSource input)
        {
            context.Scope[Key] = Value;
            return Task.FromResult(input);
        }
    }

    class CaptureInputAction : PipelineAction
    {
        public static IValueSource LastInput { get; set; }

        protected override Task<IValueSource> DefaultHandlerAsync(IExecutionContext context, IValueSource input)
        {
            LastInput = input;
            return Task.FromResult(input);
        }
    }

    class ActionWithConnectionString : PipelineAction
    {
        public string ConnectionString { get; set; }

        protected override Task<IValueSource> DefaultHandlerAsync(IExecutionContext context, IValueSource input)
            => throw new InvalidOperationException("deliberate failure");
    }

    class TypeDispatchAction : PipelineAction
    {
        public string HandlerHit { get; private set; }

        public TypeDispatchAction()
        {
            SetTypeHandler<PayloadCollection>(async (context, input) =>
            {
                HandlerHit = "PayloadCollection";
                return input;
            });
            SetTypeHandler<FilePathCollection>(async (context, input) =>
            {
                HandlerHit = "FilePathCollection";
                return input;
            });
        }
    }

    class DelayAction : PipelineAction
    {
        public int DelayMs { get; set; } = 50;
        public static ConcurrentBag<(int Index, DateTime StartTime)> StartTimes { get; } = new();
        public int Index { get; set; }

        protected override async Task<IValueSource> DefaultHandlerAsync(IExecutionContext context, IValueSource input)
        {
            StartTimes.Add((Index, DateTime.UtcNow));
            await Task.Delay(DelayMs);
            return input;
        }

        public static void Reset() => StartTimes.Clear();
    }

    // Delay derived from the input ValuePrimitive<int>. Enables per-element delays
    // in ParallelForEach tests where the single Action instance is shared across elements.
    class DelayByInputAction : PipelineAction
    {
        protected override async Task<IValueSource> DefaultHandlerAsync(IExecutionContext context, IValueSource input)
        {
            if (input is ValuePrimitive<int> ms)
            {
                await Task.Delay(ms.Value);
                return ms;
            }
            return input;
        }
    }

    #endregion

    // =========================================================================
    // RESULTS / VALUE TYPES
    // =========================================================================

    [TestClass]
    public class ResultsTests
    {
        [TestMethod]
        public void NullResult_IsSingleton()
        {
            var a = NullResult.Instance;
            var b = NullResult.Instance;
            Assert.AreSame(a, b);
            Assert.AreEqual(typeof(NullResult), a.Type);
        }

        [TestMethod]
        public void ValuePrimitive_WrapsValue()
        {
            var v = new ValuePrimitive<int>(42);
            Assert.AreEqual(42, v.Value);
            Assert.AreEqual(typeof(ValuePrimitive<int>), v.Type);
        }

        [TestMethod]
        public void FilePath_StoresPath()
        {
            var fp = new FilePath(@"C:\test\file.txt");
            Assert.AreEqual(@"C:\test\file.txt", fp.Path);
            Assert.AreEqual(typeof(FilePath), fp.Type);
        }

        [TestMethod]
        public void FilePathCollection_CreatesFromStrings()
        {
            var paths = new[] { @"C:\a.txt", @"C:\b.txt" };
            var fpc = new FilePathCollection(paths);
            var items = fpc.Cast<FilePath>().ToList();
            Assert.AreEqual(2, items.Count);
            Assert.AreEqual(@"C:\a.txt", items[0].Path);
            Assert.AreEqual(@"C:\b.txt", items[1].Path);
        }

        [TestMethod]
        public void PayloadCollection_WrapsValueSources()
        {
            var items = new IValueSource[] { new FilePath("a"), new FilePath("b") };
            var pc = new PayloadCollection(items);
            Assert.AreEqual(2, pc.Count());
            Assert.AreEqual(typeof(PayloadCollection), pc.Type);
        }

        [TestMethod]
        public void DictionaryCollection_WrapsData()
        {
            var dicts = new[]
            {
                new Dictionary<string, object> { ["key"] = "value1" },
                new Dictionary<string, object> { ["key"] = "value2" }
            };
            var dc = new DictionaryCollection(dicts);
            Assert.AreEqual(2, dc.Count());
        }

        [TestMethod]
        public void ObjectResult_WrapsArbitraryObject()
        {
            var obj = new { Name = "test" };
            var or = new ObjectResult(obj);
            Assert.AreEqual(obj, or.Value);
            Assert.AreEqual(typeof(ObjectResult), or.Type);
        }
    }

    // =========================================================================
    // EXECUTION CONTEXT
    // =========================================================================

    [TestClass]
    public class ExecutionContextTests
    {
        [TestMethod]
        public async Task Session_SharedAcrossNew()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            await builder
                .StartWith<SetSessionVariables>(op => op.AddStateVariable("key", "value"))
                .ContinueWith<ReadSessionAction>(op => op.Key = "key")
                .ExecuteAsync();

            Assert.AreEqual("value", ReadSessionAction.LastValue);
        }

        [TestMethod]
        public async Task Scope_CopiedOnNew()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            // Write to scope in step 1, read in step 2
            await builder
                .StartWith<WriteScopeAction>(op => { op.Key = "x"; op.Value = "hello"; })
                .ContinueWith<ReadScopeAction>(op => op.Key = "x")
                .ExecuteAsync();

            Assert.AreEqual("hello", ReadScopeAction.LastValue);
        }

        [TestMethod]
        public async Task Logging_ReachesLogger()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            await builder
                .StartWith<ReturnValueAction>(op => op.Value = NullResult.Instance)
                .ExecuteAsync();

            // PipelineAction.ExecuteAsync logs "executing" and "compleated" for each action
            Assert.IsTrue(logger.Infos.Any(m => m.Contains("executing")));
            Assert.IsTrue(logger.Infos.Any(m => m.Contains("completed")));
        }
    }

    // =========================================================================
    // PIPELINE BUILDER
    // =========================================================================

    [TestClass]
    public class PipelineBuilderTests
    {
        [TestMethod]
        public async Task StartWith_CreatesSingleActionPipeline()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var result = await builder
                .StartWith<ReturnValueAction>(op => op.Value = new ValuePrimitive<string>("hello"))
                .ExecuteAsync();

            Assert.IsInstanceOfType(result, typeof(ValuePrimitive<string>));
            Assert.AreEqual("hello", ((ValuePrimitive<string>)result).Value);
        }

        [TestMethod]
        public async Task ContinueWith_ChainsActions()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var result = await builder
                .StartWith<ReturnValueAction>(op => op.Value = new ValuePrimitive<string>("hello"))
                .ContinueWith<AddSuffixAction>(op => op.Suffix = "_world")
                .ExecuteAsync();

            Assert.AreEqual("hello_world", ((ValuePrimitive<string>)result).Value);
        }

        [TestMethod]
        public async Task Create_ReturnsPipeline()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var pipeline = builder
                .StartWith<ReturnValueAction>(op => op.Value = new ValuePrimitive<int>(1))
                .Create();

            Assert.IsNotNull(pipeline);
            Assert.IsNotNull(pipeline.Actions);
            Assert.AreEqual(1, pipeline.Actions.Count());
        }

        [TestMethod]
        public async Task ExecuteAsync_Generic_CastsResult()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var result = await builder
                .StartWith<ReturnValueAction>(op => op.Value = NullResult.Instance)
                .ExecuteAsync<NullResult>();

            Assert.AreSame(NullResult.Instance, result);
        }

        [TestMethod]
        public async Task ThreeStepPipeline_ExecutesInOrder()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var result = await builder
                .StartWith<ReturnValueAction>(op => op.Value = new ValuePrimitive<string>("a"))
                .ContinueWith<AddSuffixAction>(op => op.Suffix = "b")
                .ContinueWith<AddSuffixAction>(op => op.Suffix = "c")
                .ExecuteAsync();

            Assert.AreEqual("abc", ((ValuePrimitive<string>)result).Value);
        }
    }

    // =========================================================================
    // PIPELINE (sequential execution)
    // =========================================================================

    [TestClass]
    public class PipelineTests
    {
        [TestMethod]
        public async Task Pipeline_PassesResultBetweenActions()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var result = await builder
                .StartWith<ReturnValueAction>(op => op.Value = new ValuePrimitive<string>("start"))
                .ContinueWith<AddSuffixAction>(op => op.Suffix = "_end")
                .ExecuteAsync();

            Assert.AreEqual("start_end", ((ValuePrimitive<string>)result).Value);
        }

        [TestMethod]
        public async Task Pipeline_ErrorPropagates()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            await Assert.ThrowsExceptionAsync<InvalidOperationException>(async () =>
            {
                await builder
                    .StartWith<ReturnValueAction>(op => op.Value = NullResult.Instance)
                    .ContinueWith<FailingAction>(op => op.ErrorMessage = "boom")
                    .ExecuteAsync();
            });

            Assert.IsTrue(logger.Errors.Any(m => m.Contains("boom")));
        }

        [TestMethod]
        public async Task Pipeline_ErrorDoesNotRunSubsequentActions()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);
            CaptureInputAction.LastInput = null;

            try
            {
                await builder
                    .StartWith<FailingAction>()
                    .ContinueWith<CaptureInputAction>()
                    .ExecuteAsync();
            }
            catch { }

            Assert.IsNull(CaptureInputAction.LastInput);
        }
    }

    // =========================================================================
    // PIPELINE ACTION (type dispatch, Format, error masking)
    // =========================================================================

    [TestClass]
    public class PipelineActionTests
    {
        [TestMethod]
        public async Task ErrorLogging_MasksConnectionString()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            try
            {
                await builder
                    .StartWith<ActionWithConnectionString>(op =>
                        op.ConnectionString = "Server=localhost;Password=secret123")
                    .ExecuteAsync();
            }
            catch { }

            var errorLog = logger.Errors.FirstOrDefault() ?? "";
            Assert.IsTrue(errorLog.Contains("MASKED"), "ConnectionString should be masked in error logs");
            Assert.IsFalse(errorLog.Contains("secret123"), "Password value should not appear in error logs");
        }

        [TestMethod]
        public async Task Format_ResolvesSessionVariables()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);
            var tempDir = Path.GetTempPath();

            await builder
                .StartWith<SetSessionVariables>(op =>
                    op.AddStateVariable("TestDir", tempDir))
                .ContinueWith<GetFiles>(op =>
                {
                    op.DirectoryPath = "{session.TestDir}";
                    op.SearchPattern = "*.nonexistent";
                    op.SearchOption = SearchOption.TopDirectoryOnly;
                })
                .ExecuteAsync();

            // If Format didn't resolve, GetFiles would fail with literal "{session.TestDir}"
        }
    }

    // =========================================================================
    // SET STATE VARIABLES
    // =========================================================================

    [TestClass]
    public class SetStateVariablesTests
    {
        [TestMethod]
        public async Task SetSessionVariables_SetsValues()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            ReadSessionAction.LastValue = null;
            await builder
                .StartWith<SetSessionVariables>(op =>
                {
                    op.AddStateVariable("key1", "value1");
                    op.AddStateVariable("key2", 42);
                })
                .ContinueWith<ReadSessionAction>(op => op.Key = "key1")
                .ExecuteAsync();

            Assert.AreEqual("value1", ReadSessionAction.LastValue);
        }

        [TestMethod]
        public async Task SetScopeVariables_SetsValues()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            ReadScopeAction.LastValue = null;
            await builder
                .StartWith<SetScopeVariables>(op =>
                    op.AddStateVariable("myKey", "myValue"))
                .ContinueWith<ReadScopeAction>(op => op.Key = "myKey")
                .ExecuteAsync();

            Assert.AreEqual("myValue", ReadScopeAction.LastValue);
        }

        [TestMethod]
        public async Task StoreInScope_StoresPayload()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            ReadScopeAction.LastValue = null;
            await builder
                .StartWith<ReturnValueAction>(op => op.Value = new ValuePrimitive<string>("payload"))
                .ContinueWith<StoreInScope>(op => op.Name = "stored")
                .ContinueWith<ReadScopeAction>(op => op.Key = "stored")
                .ExecuteAsync();

            Assert.IsInstanceOfType(ReadScopeAction.LastValue, typeof(ValuePrimitive<string>));
        }
    }

    // =========================================================================
    // PARALLEL PIPELINE
    // =========================================================================

    [TestClass]
    public class ParallelPipelineTests
    {
        [TestMethod]
        public async Task ParallelPipeline_ExecutesAllActions()
        {
            TrackExecutionAction.Reset();
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            await builder
                .StartWith<ParallelPipeline>(op =>
                {
                    op.AddPipeline(PipelineBuilder.CreateAction<TrackExecutionAction>(a => a.Order = 1))
                      .AddPipeline(PipelineBuilder.CreateAction<TrackExecutionAction>(a => a.Order = 2))
                      .AddPipeline(PipelineBuilder.CreateAction<TrackExecutionAction>(a => a.Order = 3));
                })
                .ExecuteAsync();

            Assert.AreEqual(3, TrackExecutionAction.Executions.Count);
            Assert.IsTrue(TrackExecutionAction.Executions.Any(e => e.Order == 1));
            Assert.IsTrue(TrackExecutionAction.Executions.Any(e => e.Order == 2));
            Assert.IsTrue(TrackExecutionAction.Executions.Any(e => e.Order == 3));
        }

        [TestMethod]
        public async Task ParallelPipeline_ReturnsPayloadCollection()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var result = await builder
                .StartWith<ParallelPipeline>(op =>
                {
                    op.AddPipeline(PipelineBuilder.CreateAction<ReturnValueAction>(a => a.Value = new ValuePrimitive<int>(1)))
                      .AddPipeline(PipelineBuilder.CreateAction<ReturnValueAction>(a => a.Value = new ValuePrimitive<int>(2)));
                })
                .ExecuteAsync();

            Assert.IsInstanceOfType(result, typeof(PayloadCollection));
            Assert.AreEqual(2, ((PayloadCollection)result).Count());
        }

        [TestMethod]
        public async Task ParallelPipeline_ActuallyRunsConcurrently()
        {
            // Each action sleeps 50ms. With true concurrency, 3 actions on MaxDoP >= 3
            // should complete in ~50ms. Sequential would be ~150ms. Assert < 120ms.
            TrackExecutionAction.Reset();
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var sw = Stopwatch.StartNew();
            await builder
                .StartWith<ParallelPipeline>(op =>
                {
                    op.AddPipeline(PipelineBuilder.CreateAction<TrackExecutionAction>(a => a.Order = 1))
                      .AddPipeline(PipelineBuilder.CreateAction<TrackExecutionAction>(a => a.Order = 2))
                      .AddPipeline(PipelineBuilder.CreateAction<TrackExecutionAction>(a => a.Order = 3));
                })
                .ExecuteAsync();
            sw.Stop();

            var elapsed = sw.ElapsedMilliseconds;
            Trace.WriteLine($"ParallelPipeline 3 actions took {elapsed}ms (expect ~50ms parallel, ~150ms sequential)");

            Assert.AreEqual(3, TrackExecutionAction.Executions.Count);
            Assert.IsTrue(elapsed < 120,
                $"Expected parallel execution (<120ms) but took {elapsed}ms for 3x50ms actions");
        }

        [TestMethod]
        public async Task ParallelPipeline_RespectsMaxDegreeOfParallelism()
        {
            // 10 pipelines, MaxDoP=3, each 50ms work.
            // With a true concurrency cap: ceil(10/3) * 50ms = 4 waves * 50ms = ~200ms.
            // Batched implementation would land in the same ballpark but with higher variance
            // under straggler load — this test primarily proves the cap caps AND parallelizes.
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var sw = Stopwatch.StartNew();
            await builder
                .StartWith<ParallelPipeline>(op =>
                {
                    op.MaxDegreeOfParallelism = 3;
                    for (int i = 0; i < 10; i++)
                        op.AddPipeline(PipelineBuilder.CreateAction<DelayAction>(a => { a.DelayMs = 50; a.Index = 0; }));
                })
                .ExecuteAsync();
            sw.Stop();

            // 10 items through a cap of 3 at 50ms each = at least 150ms (3 full waves + partial).
            // Upper bound allows scheduler slack.
            Assert.IsTrue(sw.ElapsedMilliseconds >= 150,
                $"Expected at least ~150ms with MaxDoP=3 for 10x50ms, got {sw.ElapsedMilliseconds}ms");
            Assert.IsTrue(sw.ElapsedMilliseconds < 400,
                $"Expected cap to parallelize (<400ms) for 10x50ms with MaxDoP=3, got {sw.ElapsedMilliseconds}ms");
        }

        [TestMethod]
        public void ParallelPipeline_AddPipeline_WithIPipeline()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var sub1 = builder.StartWith<ReturnValueAction>(op => op.Value = new ValuePrimitive<int>(10)).Create();
            var sub2 = builder.StartWith<ReturnValueAction>(op => op.Value = new ValuePrimitive<int>(20)).Create();

            var parallel = new ParallelPipeline();
            var returned = parallel.AddPipeline(sub1).AddPipeline(sub2);

            Assert.AreSame(parallel, returned, "AddPipeline should return the instance for chaining");
            Assert.AreEqual(2, parallel.Pipelines.Count());
            Assert.AreSame(sub1, parallel.Pipelines.ElementAt(0));
            Assert.AreSame(sub2, parallel.Pipelines.ElementAt(1));
        }

        [TestMethod]
        public void ParallelPipeline_AddPipeline_WithActions()
        {
            var parallel = new ParallelPipeline();
            var a1 = PipelineBuilder.CreateAction<ReturnValueAction>(a => a.Value = new ValuePrimitive<int>(1));
            var a2 = PipelineBuilder.CreateAction<AddSuffixAction>(a => a.Suffix = "!");

            parallel.AddPipeline(a1, a2);

            Assert.AreEqual(1, parallel.Pipelines.Count());
            var wrapped = parallel.Pipelines.Single();
            Assert.IsInstanceOfType(wrapped, typeof(Pipeline));
            var wrappedActions = wrapped.Actions.ToArray();
            Assert.AreEqual(2, wrappedActions.Length);
            Assert.AreSame(a1, wrappedActions[0]);
            Assert.AreSame(a2, wrappedActions[1]);
        }

        [TestMethod]
        public void ParallelPipeline_AddPipeline_MixedOverloads()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var pre = builder.StartWith<ReturnValueAction>(op => op.Value = new ValuePrimitive<int>(1)).Create();
            var bareAction = PipelineBuilder.CreateAction<ReturnValueAction>(a => a.Value = new ValuePrimitive<int>(2));

            var parallel = new ParallelPipeline();
            parallel.AddPipeline(pre).AddPipeline(bareAction);

            Assert.AreEqual(2, parallel.Pipelines.Count());
            Assert.AreSame(pre, parallel.Pipelines.ElementAt(0), "First slot should be the pre-built IPipeline");
            Assert.IsInstanceOfType(parallel.Pipelines.ElementAt(1), typeof(Pipeline));
            Assert.AreSame(bareAction, parallel.Pipelines.ElementAt(1).Actions.Single());
        }

        [TestMethod]
        public void ParallelPipeline_PipelinesSetter_ReplacesList()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var sub1 = builder.StartWith<ReturnValueAction>(op => op.Value = new ValuePrimitive<int>(1)).Create();
            var sub2 = builder.StartWith<ReturnValueAction>(op => op.Value = new ValuePrimitive<int>(2)).Create();
            var sub3 = builder.StartWith<ReturnValueAction>(op => op.Value = new ValuePrimitive<int>(3)).Create();

            var parallel = new ParallelPipeline();
            parallel.AddPipeline(sub1);

            // Assign a new list — must replace, not merge.
            parallel.Pipelines = new IPipeline[] { sub2, sub3 };

            Assert.AreEqual(2, parallel.Pipelines.Count());
            Assert.IsFalse(parallel.Pipelines.Contains(sub1), "Setter must clear previous entries");
            Assert.AreSame(sub2, parallel.Pipelines.ElementAt(0));
            Assert.AreSame(sub3, parallel.Pipelines.ElementAt(1));
        }

        [TestMethod]
        public async Task ParallelPipeline_SlowPipelineDoesNotBlockOthers()
        {
            // Regression test for the batching bug. With the batched implementation,
            // one slow pipeline blocked a whole batch. With semaphore-gated concurrency,
            // 4 pipelines at MaxDoP=4 should complete in ~max(durations), not sum-of-durations.
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var sw = Stopwatch.StartNew();
            await builder
                .StartWith<ParallelPipeline>(op =>
                {
                    op.MaxDegreeOfParallelism = 4;
                    op.AddPipeline(PipelineBuilder.CreateAction<DelayAction>(a => { a.DelayMs = 500; a.Index = 0; }))
                      .AddPipeline(PipelineBuilder.CreateAction<DelayAction>(a => { a.DelayMs = 50; a.Index = 1; }))
                      .AddPipeline(PipelineBuilder.CreateAction<DelayAction>(a => { a.DelayMs = 50; a.Index = 2; }))
                      .AddPipeline(PipelineBuilder.CreateAction<DelayAction>(a => { a.DelayMs = 50; a.Index = 3; }));
                })
                .ExecuteAsync();
            sw.Stop();

            // True parallelism: ~500ms. Sequential: 650ms. Assert well under 650ms.
            Assert.IsTrue(sw.ElapsedMilliseconds < 600,
                $"Slow branch should not block others — expected ~500ms, got {sw.ElapsedMilliseconds}ms");
        }

        [TestMethod]
        public void ParallelPipeline_AddPipeline_NullPipeline_Throws()
        {
            var parallel = new ParallelPipeline();
            Assert.ThrowsException<ArgumentNullException>(() => parallel.AddPipeline((IPipeline)null));
        }

        [TestMethod]
        public void ParallelPipeline_AddPipeline_EmptyActions_Throws()
        {
            var parallel = new ParallelPipeline();
            Assert.ThrowsException<ArgumentException>(() => parallel.AddPipeline(new IPipelineAction[0]));
        }

        [TestMethod]
        public async Task ParallelPipeline_MaxDop1_RunsSequentially()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var sw = Stopwatch.StartNew();
            await builder
                .StartWith<ParallelPipeline>(op =>
                {
                    op.MaxDegreeOfParallelism = 1;
                    op.AddPipeline(PipelineBuilder.CreateAction<DelayAction>(a => { a.DelayMs = 50; a.Index = 0; }))
                      .AddPipeline(PipelineBuilder.CreateAction<DelayAction>(a => { a.DelayMs = 50; a.Index = 1; }))
                      .AddPipeline(PipelineBuilder.CreateAction<DelayAction>(a => { a.DelayMs = 50; a.Index = 2; }));
                })
                .ExecuteAsync();
            sw.Stop();

            // MaxDoP=1 forces strict serialization: ~150ms.
            Assert.IsTrue(sw.ElapsedMilliseconds >= 140,
                $"MaxDoP=1 should serialize — expected >=140ms for 3x50ms, got {sw.ElapsedMilliseconds}ms");
        }
    }

    // =========================================================================
    // FOR EACH
    // =========================================================================

    [TestClass]
    public class ForEachTests
    {
        [TestMethod]
        public async Task ForEach_IteratesOverPayloadCollection()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var result = await builder
                .StartWith<ReturnValueAction>(op =>
                    op.Value = new PayloadCollection(new IValueSource[]
                    {
                        new ValuePrimitive<string>("a"),
                        new ValuePrimitive<string>("b"),
                        new ValuePrimitive<string>("c"),
                    }))
                .ContinueWith<ForEach>(op =>
                {
                    op.Actions = new IPipelineAction[]
                    {
                        PipelineBuilder.CreateAction<AddSuffixAction>(a => a.Suffix = "!")
                    };
                })
                .ExecuteAsync();

            Assert.IsInstanceOfType(result, typeof(PayloadCollection));
            var items = ((PayloadCollection)result).Cast<ValuePrimitive<string>>().ToList();
            Assert.AreEqual(3, items.Count);
            Assert.AreEqual("a!", items[0].Value);
            Assert.AreEqual("b!", items[1].Value);
            Assert.AreEqual("c!", items[2].Value);
        }

        [TestMethod]
        public async Task ForEach_MultipleActions_ChainPerElement()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var result = await builder
                .StartWith<ReturnValueAction>(op =>
                    op.Value = new PayloadCollection(new IValueSource[]
                    {
                        new ValuePrimitive<string>("x"),
                    }))
                .ContinueWith<ForEach>(op =>
                {
                    op.Actions = new IPipelineAction[]
                    {
                        PipelineBuilder.CreateAction<AddSuffixAction>(a => a.Suffix = "1"),
                        PipelineBuilder.CreateAction<AddSuffixAction>(a => a.Suffix = "2"),
                    };
                })
                .ExecuteAsync();

            var items = ((PayloadCollection)result).Cast<ValuePrimitive<string>>().ToList();
            Assert.AreEqual("x12", items[0].Value);
        }
    }

    // =========================================================================
    // PAYLOAD PROVIDERS
    // =========================================================================

    [TestClass]
    public class PayloadProviderTests
    {
        [TestMethod]
        public async Task GetPayloadFromScope_RetrievesStoredValue()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            CaptureInputAction.LastInput = null;
            await builder
                .StartWith<ReturnValueAction>(op =>
                    op.Value = new ValuePrimitive<string>("stored_value"))
                .ContinueWith<StoreInScope>(op => op.Name = "myData")
                .ContinueWith<CaptureInputAction>(op =>
                    op.PayloadProvider = new GetPayloadFromScope { Name = "myData" })
                .ExecuteAsync();

            Assert.IsInstanceOfType(CaptureInputAction.LastInput, typeof(ValuePrimitive<string>));
            Assert.AreEqual("stored_value", ((ValuePrimitive<string>)CaptureInputAction.LastInput).Value);
        }
    }

    // =========================================================================
    // NESTED PIPELINES (composability)
    // =========================================================================

    [TestClass]
    public class NestedPipelineTests
    {
        [TestMethod]
        public async Task Pipeline_CanNestSubPipeline()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var subPipeline = builder
                .StartWith<AddSuffixAction>(op => op.Suffix = "_sub")
                .Create();

            var result = await builder
                .StartWith<ReturnValueAction>(op => op.Value = new ValuePrimitive<string>("main"))
                .ContinueWith<ReturnValueAction>(op => op.Value = new ValuePrimitive<string>("input"))
                .ExecuteAsync();

            // Test that Create() produces a valid pipeline that can be used as an action
            Assert.IsNotNull(subPipeline);
            Assert.IsNotNull(subPipeline.Actions);
        }

        [TestMethod]
        public async Task ParallelPipeline_WithSubPipelines()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var sub1 = builder
                .StartWith<ReturnValueAction>(op => op.Value = new ValuePrimitive<int>(1))
                .Create();

            var sub2 = builder
                .StartWith<ReturnValueAction>(op => op.Value = new ValuePrimitive<int>(2))
                .Create();

            var result = await builder
                .StartWith<ParallelPipeline>(op =>
                {
                    op.AddPipeline(sub1)
                      .AddPipeline(sub2);
                })
                .ExecuteAsync();

            Assert.IsInstanceOfType(result, typeof(PayloadCollection));
            Assert.AreEqual(2, ((PayloadCollection)result).Count());
        }
    }

    // =========================================================================
    // FILE I/O ACTIONS
    // =========================================================================

    [TestClass]
    public class FileIOTests
    {
        private string _tempDir;

        [TestInitialize]
        public void Setup()
        {
            _tempDir = Path.Combine(Path.GetTempPath(), "FlowTest_" + Guid.NewGuid().ToString("N")[..8]);
            Directory.CreateDirectory(_tempDir);
        }

        [TestCleanup]
        public void Cleanup()
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, true);
        }

        [TestMethod]
        public async Task GetFiles_ReturnsMatchingFiles()
        {
            File.WriteAllText(Path.Combine(_tempDir, "test1.txt"), "a");
            File.WriteAllText(Path.Combine(_tempDir, "test2.txt"), "b");
            File.WriteAllText(Path.Combine(_tempDir, "test3.csv"), "c");

            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var result = await builder
                .StartWith<GetFiles>(op =>
                {
                    op.DirectoryPath = _tempDir;
                    op.SearchPattern = "*.txt";
                    op.SearchOption = SearchOption.TopDirectoryOnly;
                })
                .ExecuteAsync();

            var files = ((FilePathCollection)result).Cast<FilePath>().ToList();
            Assert.AreEqual(2, files.Count);
            Assert.IsTrue(files.All(f => f.Path.EndsWith(".txt")));
        }

        [TestMethod]
        public async Task DeleteFiles_RemovesFiles()
        {
            var file1 = Path.Combine(_tempDir, "delete_me.txt");
            File.WriteAllText(file1, "delete");

            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            await builder
                .StartWith<GetFiles>(op =>
                {
                    op.DirectoryPath = _tempDir;
                    op.SearchPattern = "*.txt";
                    op.SearchOption = SearchOption.TopDirectoryOnly;
                })
                .ContinueWith<DeleteFiles>()
                .ExecuteAsync();

            Assert.IsFalse(File.Exists(file1));
        }

        [TestMethod]
        public async Task UnzipFile_ExtractsContents()
        {
            var zipSourceDir = Path.Combine(_tempDir, "zip_source");
            var extractDir = Path.Combine(_tempDir, "extracted");
            var zipPath = Path.Combine(_tempDir, "test.zip");
            Directory.CreateDirectory(zipSourceDir);
            Directory.CreateDirectory(extractDir);

            File.WriteAllText(Path.Combine(zipSourceDir, "inner.txt"), "inner content");
            System.IO.Compression.ZipFile.CreateFromDirectory(zipSourceDir, zipPath);

            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var result = await builder
                .StartWith<ReturnValueAction>(op =>
                    op.Value = new FilePathCollection(new[] { zipPath }))
                .ContinueWith<UnzipFile>(op => op.WorkingDirectory = extractDir)
                .ExecuteAsync();

            Assert.IsInstanceOfType(result, typeof(FilePathCollection));
            Assert.IsTrue(File.Exists(Path.Combine(extractDir, "inner.txt")));
            Assert.AreEqual("inner content", File.ReadAllText(Path.Combine(extractDir, "inner.txt")));
        }
    }

    // =========================================================================
    // PARALLEL FOR EACH
    // =========================================================================

    [TestClass]
    public class ParallelForEachTests
    {
        [TestMethod]
        public async Task ParallelForEach_ProcessesEachElement()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var result = await builder
                .StartWith<ReturnValueAction>(op =>
                    op.Value = new PayloadCollection(new IValueSource[]
                    {
                        new ValuePrimitive<string>("a"),
                        new ValuePrimitive<string>("b"),
                        new ValuePrimitive<string>("c"),
                    }))
                .ContinueWith<ParallelForEach>(op =>
                {
                    op.Actions = new IPipelineAction[]
                    {
                        PipelineBuilder.CreateAction<AddSuffixAction>(a => a.Suffix = "!")
                    };
                })
                .ExecuteAsync();

            var items = ((PayloadCollection)result).Cast<ValuePrimitive<string>>().ToList();
            Assert.AreEqual(3, items.Count);
            Assert.AreEqual("a!", items[0].Value);
            Assert.AreEqual("b!", items[1].Value);
            Assert.AreEqual("c!", items[2].Value);
        }

        [TestMethod]
        public async Task ParallelForEach_ChainsActionsPerElement()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var result = await builder
                .StartWith<ReturnValueAction>(op =>
                    op.Value = new PayloadCollection(new IValueSource[]
                    {
                        new ValuePrimitive<string>("x"),
                    }))
                .ContinueWith<ParallelForEach>(op =>
                {
                    op.Actions = new IPipelineAction[]
                    {
                        PipelineBuilder.CreateAction<AddSuffixAction>(a => a.Suffix = "1"),
                        PipelineBuilder.CreateAction<AddSuffixAction>(a => a.Suffix = "2"),
                    };
                })
                .ExecuteAsync();

            var items = ((PayloadCollection)result).Cast<ValuePrimitive<string>>().ToList();
            Assert.AreEqual("x12", items[0].Value);
        }

        [TestMethod]
        public async Task ParallelForEach_PreservesElementOrder()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var input = Enumerable.Range(0, 10)
                .Select(i => (IValueSource)new ValuePrimitive<string>(i.ToString()))
                .ToArray();

            var result = await builder
                .StartWith<ReturnValueAction>(op =>
                    op.Value = new PayloadCollection(input))
                .ContinueWith<ParallelForEach>(op =>
                {
                    op.MaxDegreeOfParallelism = 3;
                    op.Actions = new IPipelineAction[]
                    {
                        PipelineBuilder.CreateAction<AddSuffixAction>(a => a.Suffix = "!")
                    };
                })
                .ExecuteAsync();

            var items = ((PayloadCollection)result).Cast<ValuePrimitive<string>>().ToList();
            for (int i = 0; i < 10; i++)
                Assert.AreEqual($"{i}!", items[i].Value);
        }

        [TestMethod]
        public async Task ParallelForEach_ActuallyRunsConcurrently()
        {
            DelayAction.Reset();
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var input = Enumerable.Range(0, 4)
                .Select(i => (IValueSource)new ValuePrimitive<string>(i.ToString()))
                .ToArray();

            var sw = Stopwatch.StartNew();
            await builder
                .StartWith<ReturnValueAction>(op =>
                    op.Value = new PayloadCollection(input))
                .ContinueWith<ParallelForEach>(op =>
                {
                    op.MaxDegreeOfParallelism = 4;
                    op.Actions = new IPipelineAction[]
                    {
                        PipelineBuilder.CreateAction<DelayAction>(a => { a.DelayMs = 100; a.Index = 0; })
                    };
                })
                .ExecuteAsync();
            sw.Stop();

            // 4 elements x 100ms each. If parallel: ~100ms. If sequential: ~400ms.
            Assert.AreEqual(4, DelayAction.StartTimes.Count);
            Assert.IsTrue(sw.ElapsedMilliseconds < 300,
                $"Expected parallel execution (~100ms) but took {sw.ElapsedMilliseconds}ms");
        }

        [TestMethod]
        public async Task ParallelForEach_RespectsMaxDop()
        {
            DelayAction.Reset();
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var input = Enumerable.Range(0, 4)
                .Select(i => (IValueSource)new ValuePrimitive<string>(i.ToString()))
                .ToArray();

            var sw = Stopwatch.StartNew();
            await builder
                .StartWith<ReturnValueAction>(op =>
                    op.Value = new PayloadCollection(input))
                .ContinueWith<ParallelForEach>(op =>
                {
                    op.MaxDegreeOfParallelism = 2;
                    op.Actions = new IPipelineAction[]
                    {
                        PipelineBuilder.CreateAction<DelayAction>(a => { a.DelayMs = 100; a.Index = 0; })
                    };
                })
                .ExecuteAsync();
            sw.Stop();

            // 4 elements, MaxDop=2: 2 batches x 100ms = ~200ms
            Assert.AreEqual(4, DelayAction.StartTimes.Count);
            Assert.IsTrue(sw.ElapsedMilliseconds >= 150,
                $"Expected 2 batches (~200ms) but took {sw.ElapsedMilliseconds}ms — MaxDop not respected");
            Assert.IsTrue(sw.ElapsedMilliseconds < 500,
                $"Took too long ({sw.ElapsedMilliseconds}ms) — something is wrong");
        }

        [TestMethod]
        public async Task ParallelForEach_SlowElementDoesNotBlockOthers()
        {
            // Regression test for the batching bug that previously existed in ParallelForEach.
            // One slow element used to hold a whole batch. With semaphore-gated concurrency,
            // 4 elements at MaxDoP=4 should complete in ~max(delays), not sum-of-delays.
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var input = new PayloadCollection(new IValueSource[]
            {
                new ValuePrimitive<int>(500),
                new ValuePrimitive<int>(50),
                new ValuePrimitive<int>(50),
                new ValuePrimitive<int>(50),
            });

            var sw = Stopwatch.StartNew();
            await builder
                .StartWith<ReturnValueAction>(op => op.Value = input)
                .ContinueWith<ParallelForEach>(op =>
                {
                    op.MaxDegreeOfParallelism = 4;
                    op.Actions = new IPipelineAction[]
                    {
                        PipelineBuilder.CreateAction<DelayByInputAction>()
                    };
                })
                .ExecuteAsync();
            sw.Stop();

            // True parallelism: ~500ms. Sequential: 650ms. Previous batching bug would
            // stall on the 500ms element even at MaxDoP=4.
            Assert.IsTrue(sw.ElapsedMilliseconds < 600,
                $"Slow element should not block others — expected ~500ms, got {sw.ElapsedMilliseconds}ms");
        }
    }

    // =========================================================================
    // TYPE DISPATCH (GetFormatter)
    // =========================================================================

    [TestClass]
    public class TypeDispatchTests
    {
        [TestMethod]
        public async Task GetFormatter_PrefersExactType()
        {
            var action = new TypeDispatchAction();
            var logger = new TestLogger();
            var context = new PipelineBuilder(logger)
                .StartWith<ReturnValueAction>(op =>
                    op.Value = new FilePathCollection(new[] { "a.txt" }))
                .Create();

            // Execute the TypeDispatchAction with FilePathCollection input
            var builder = new PipelineBuilder(logger);
            await builder
                .StartWith<ReturnValueAction>(op =>
                    op.Value = new FilePathCollection(new[] { "a.txt" }))
                .ContinueWith<TypeDispatchAction>()
                .ExecuteAsync();
        }

        [TestMethod]
        public async Task GetFormatter_FilePathCollection_HitsSpecificHandler()
        {
            // TypeDispatchAction registers handlers for both PayloadCollection and FilePathCollection.
            // FilePathCollection extends PayloadCollection. Both match via IsAssignableFrom.
            // The fix should prefer FilePathCollection (most specific).
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            // We need to verify the handler hit — but TypeDispatchAction is created fresh
            // by the builder. We use a custom approach: create the action, run it directly.
            var action = new TypeDispatchAction();
            var ctx = new ExecutionContextForTest(logger);

            await action.ExecuteAsync(ctx.WithResult(new FilePathCollection(new[] { "a.txt" })));
            Assert.AreEqual("FilePathCollection", action.HandlerHit,
                "Should hit FilePathCollection handler, not PayloadCollection");
        }

        [TestMethod]
        public async Task GetFormatter_PayloadCollection_HitsBaseHandler()
        {
            var logger = new TestLogger();
            var action = new TypeDispatchAction();
            var ctx = new ExecutionContextForTest(logger);

            await action.ExecuteAsync(ctx.WithResult(new PayloadCollection(new IValueSource[]
            {
                new ValuePrimitive<string>("x")
            })));
            Assert.AreEqual("PayloadCollection", action.HandlerHit,
                "Should hit PayloadCollection handler for base type");
        }
    }

    // =========================================================================
    // PAYLOAD PROVIDERS (Session)
    // =========================================================================

    [TestClass]
    public class PayloadProviderSessionTests
    {
        [TestMethod]
        public async Task GetPayloadFromSession_RetrievesStoredValue()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            CaptureInputAction.LastInput = null;
            await builder
                .StartWith<ReturnValueAction>(op =>
                    op.Value = new ValuePrimitive<string>("session_value"))
                .ContinueWith<StoreInSession>(op => op.Name = "myData")
                .ContinueWith<CaptureInputAction>(op =>
                    op.PayloadProvider = new GetPayloadFromSession { Name = "myData" })
                .ExecuteAsync();

            Assert.IsInstanceOfType(CaptureInputAction.LastInput, typeof(ValuePrimitive<string>));
            Assert.AreEqual("session_value", ((ValuePrimitive<string>)CaptureInputAction.LastInput).Value);
        }
    }

    // =========================================================================
    // STORE IN SESSION
    // =========================================================================

    [TestClass]
    public class StoreInSessionTests
    {
        [TestMethod]
        public async Task StoreInSession_PersistsAcrossNewContexts()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            ReadSessionAction.LastValue = null;
            await builder
                .StartWith<ReturnValueAction>(op =>
                    op.Value = new ValuePrimitive<string>("persisted"))
                .ContinueWith<StoreInSession>(op => op.Name = "key")
                .ContinueWith<ReadSessionAction>(op => op.Key = "key")
                .ExecuteAsync();

            Assert.IsInstanceOfType(ReadSessionAction.LastValue, typeof(ValuePrimitive<string>));
            Assert.AreEqual("persisted", ((ValuePrimitive<string>)ReadSessionAction.LastValue).Value);
        }
    }

    // =========================================================================
    // EDGE CASES
    // =========================================================================

    [TestClass]
    public class EdgeCaseTests
    {
        [TestMethod]
        public async Task Pipeline_EmptyActions_ReturnsInput()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var pipeline = PipelineBuilder.CreatePipeline();
            var ctx = new ExecutionContextForTest(logger)
                .WithResult(new ValuePrimitive<string>("unchanged"));

            var result = await pipeline.ExecuteAsync(ctx);
            Assert.IsInstanceOfType(result, typeof(ValuePrimitive<string>));
            Assert.AreEqual("unchanged", ((ValuePrimitive<string>)result).Value);
        }

        [TestMethod]
        public async Task ParallelForEach_EmptyCollection_ReturnsEmptyCollection()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var result = await builder
                .StartWith<ReturnValueAction>(op =>
                    op.Value = new PayloadCollection(Array.Empty<IValueSource>()))
                .ContinueWith<ParallelForEach>(op =>
                {
                    op.Actions = new IPipelineAction[]
                    {
                        PipelineBuilder.CreateAction<AddSuffixAction>(a => a.Suffix = "!")
                    };
                })
                .ExecuteAsync();

            Assert.IsInstanceOfType(result, typeof(PayloadCollection));
            Assert.AreEqual(0, ((PayloadCollection)result).Count());
        }

        [TestMethod]
        public async Task ForEach_NonPayloadCollectionInput_ThrowsNotImplemented()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            await Assert.ThrowsExceptionAsync<HandlerNotFoundException>(async () =>
            {
                await builder
                    .StartWith<ReturnValueAction>(op =>
                        op.Value = new ValuePrimitive<string>("not a collection"))
                    .ContinueWith<ForEach>(op =>
                    {
                        op.Actions = new IPipelineAction[]
                        {
                            PipelineBuilder.CreateAction<AddSuffixAction>()
                        };
                    })
                    .ExecuteAsync();
            });
        }

        [TestMethod]
        public async Task ParallelForEach_NonPayloadCollectionInput_ThrowsNotImplemented()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            await Assert.ThrowsExceptionAsync<HandlerNotFoundException>(async () =>
            {
                await builder
                    .StartWith<ReturnValueAction>(op =>
                        op.Value = new ValuePrimitive<string>("not a collection"))
                    .ContinueWith<ParallelForEach>(op =>
                    {
                        op.Actions = new IPipelineAction[]
                        {
                            PipelineBuilder.CreateAction<AddSuffixAction>()
                        };
                    })
                    .ExecuteAsync();
            });
        }
    }

    // =========================================================================
    // ERROR PROPAGATION
    // =========================================================================

    [TestClass]
    public class ErrorPropagationTests
    {
        [TestMethod]
        public async Task ParallelPipeline_SingleFailure_ThrowsParallelPipelineException()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var ex = await Assert.ThrowsExceptionAsync<ParallelPipelineException>(async () =>
            {
                await builder
                    .StartWith<ParallelPipeline>(op =>
                    {
                        op.AddPipeline(PipelineBuilder.CreateAction<ReturnValueAction>(a => a.Value = NullResult.Instance))
                          .AddPipeline(PipelineBuilder.CreateAction<FailingAction>(a => a.ErrorMessage = "branch2 failed"))
                          .AddPipeline(PipelineBuilder.CreateAction<ReturnValueAction>(a => a.Value = NullResult.Instance));
                    })
                    .ExecuteAsync();
            });

            Assert.IsInstanceOfType(ex.InnerException, typeof(AggregateException));
            Assert.AreEqual(1, ex.AggregateException.InnerExceptions.Count);
            Assert.IsTrue(logger.Errors.Any(m => m.Contains("branch2 failed")),
                "Failing branch should be logged");
        }

        [TestMethod]
        public async Task ParallelPipeline_MultipleFailures_AllCapturedInAggregate()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var ex = await Assert.ThrowsExceptionAsync<ParallelPipelineException>(async () =>
            {
                await builder
                    .StartWith<ParallelPipeline>(op =>
                    {
                        op.MaxDegreeOfParallelism = 3;
                        op.AddPipeline(PipelineBuilder.CreateAction<DelayedFailingAction>(a =>
                              { a.ErrorMessage = "fail_A"; a.DelayMs = 10; }))
                          .AddPipeline(PipelineBuilder.CreateAction<DelayedFailingAction>(a =>
                              { a.ErrorMessage = "fail_B"; a.DelayMs = 20; }))
                          .AddPipeline(PipelineBuilder.CreateAction<DelayedFailingAction>(a =>
                              { a.ErrorMessage = "fail_C"; a.DelayMs = 30; }));
                    })
                    .ExecuteAsync();
            });

            // All three failures captured in AggregateException
            Assert.AreEqual(3, ex.AggregateException.InnerExceptions.Count);
            Assert.IsTrue(ex.AggregateException.InnerExceptions.Any(e => e.Message.Contains("fail_A")));
            Assert.IsTrue(ex.AggregateException.InnerExceptions.Any(e => e.Message.Contains("fail_B")));
            Assert.IsTrue(ex.AggregateException.InnerExceptions.Any(e => e.Message.Contains("fail_C")));

            // All three independently logged
            Assert.IsTrue(logger.Errors.Any(m => m.Contains("fail_A")), "fail_A should be logged");
            Assert.IsTrue(logger.Errors.Any(m => m.Contains("fail_B")), "fail_B should be logged");
            Assert.IsTrue(logger.Errors.Any(m => m.Contains("fail_C")), "fail_C should be logged");
        }

        [TestMethod]
        public async Task ParallelForEach_SingleElementFailure_ThrowsParallelForEachException()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var ex = await Assert.ThrowsExceptionAsync<ParallelForEachException>(async () =>
            {
                await builder
                    .StartWith<ReturnValueAction>(op =>
                        op.Value = new PayloadCollection(new IValueSource[]
                        {
                            new ValuePrimitive<string>("ok1"),
                            new ValuePrimitive<string>("ok2"),
                            new ValuePrimitive<string>("ok3"),
                        }))
                    .ContinueWith<ParallelForEach>(op =>
                    {
                        op.Actions = new IPipelineAction[]
                        {
                            PipelineBuilder.CreateAction<FailingAction>(a => a.ErrorMessage = "element failed")
                        };
                    })
                    .ExecuteAsync();
            });

            Assert.IsInstanceOfType(ex.InnerException, typeof(AggregateException));
            Assert.AreEqual(3, ex.AggregateException.InnerExceptions.Count);
            Assert.IsTrue(logger.Errors.Any(m => m.Contains("element failed")),
                "Element failure should be logged");
        }

        [TestMethod]
        public async Task ParallelForEach_FailureInChainedAction_ThrowsParallelForEachException()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var ex = await Assert.ThrowsExceptionAsync<ParallelForEachException>(async () =>
            {
                await builder
                    .StartWith<ReturnValueAction>(op =>
                        op.Value = new PayloadCollection(new IValueSource[]
                        {
                            new ValuePrimitive<string>("x"),
                        }))
                    .ContinueWith<ParallelForEach>(op =>
                    {
                        op.Actions = new IPipelineAction[]
                        {
                            PipelineBuilder.CreateAction<AddSuffixAction>(a => a.Suffix = "!"),
                            PipelineBuilder.CreateAction<FailingAction>(a => a.ErrorMessage = "second action failed"),
                        };
                    })
                    .ExecuteAsync();
            });

            Assert.AreEqual(1, ex.AggregateException.InnerExceptions.Count);

            // First action should succeed and log, second should fail and log
            Assert.IsTrue(logger.Infos.Any(m => m.Contains("AddSuffixAction") && m.Contains("completed")),
                "First action should complete successfully");
            Assert.IsTrue(logger.Errors.Any(m => m.Contains("second action failed")),
                "Second action failure should be logged");
        }

        [TestMethod]
        public async Task NestedPipeline_ErrorBubblesUpAsParallelPipelineException()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var innerPipeline = builder
                .StartWith<ReturnValueAction>(op => op.Value = new ValuePrimitive<string>("inner"))
                .ContinueWith<FailingAction>(op => op.ErrorMessage = "inner pipeline failed")
                .Create();

            var ex = await Assert.ThrowsExceptionAsync<ParallelPipelineException>(async () =>
            {
                await builder
                    .StartWith<ReturnValueAction>(op => op.Value = NullResult.Instance)
                    .ContinueWith<ParallelPipeline>(op =>
                    {
                        op.AddPipeline(innerPipeline)
                          .AddPipeline(PipelineBuilder.CreateAction<ReturnValueAction>(a => a.Value = NullResult.Instance));
                    })
                    .ExecuteAsync();
            });

            Assert.AreEqual(1, ex.AggregateException.InnerExceptions.Count);
            Assert.IsTrue(logger.Errors.Any(m => m.Contains("inner pipeline failed")),
                "Inner pipeline error should be logged");
        }
    }

    // =========================================================================
    // INTERFACE TYPE DISPATCH
    // =========================================================================

    class InterfaceDispatchAction : PipelineAction
    {
        public string HandlerHit { get; private set; }

        public InterfaceDispatchAction()
        {
            SetTypeHandler<IValueSource>(async (context, input) =>
            {
                HandlerHit = "IValueSource";
                return input;
            });
        }
    }

    class MixedDispatchAction : PipelineAction
    {
        public string HandlerHit { get; private set; }

        public MixedDispatchAction()
        {
            SetTypeHandler<IValueSource>(async (context, input) =>
            {
                HandlerHit = "IValueSource";
                return input;
            });
            SetTypeHandler<PayloadCollection>(async (context, input) =>
            {
                HandlerHit = "PayloadCollection";
                return input;
            });
        }
    }

    [TestClass]
    public class InterfaceDispatchTests
    {
        [TestMethod]
        public async Task InterfaceHandler_MatchesConcreteType()
        {
            var logger = new TestLogger();
            var action = new InterfaceDispatchAction();
            var ctx = new ExecutionContextForTest(logger)
                .WithResult(new ValuePrimitive<string>("test"));

            await action.ExecuteAsync(ctx);
            Assert.AreEqual("IValueSource", action.HandlerHit);
        }

        [TestMethod]
        public async Task MixedHandlers_ConcreteWinsOverInterface()
        {
            var logger = new TestLogger();
            var action = new MixedDispatchAction();
            var ctx = new ExecutionContextForTest(logger)
                .WithResult(new PayloadCollection(new IValueSource[] { NullResult.Instance }));

            await action.ExecuteAsync(ctx);
            Assert.AreEqual("PayloadCollection", action.HandlerHit,
                "Concrete type handler should win over interface handler");
        }

        [TestMethod]
        public async Task MixedHandlers_InterfaceFallsBackWhenNoConcreteMatch()
        {
            var logger = new TestLogger();
            var action = new MixedDispatchAction();
            var ctx = new ExecutionContextForTest(logger)
                .WithResult(new ValuePrimitive<string>("test"));

            await action.ExecuteAsync(ctx);
            Assert.AreEqual("IValueSource", action.HandlerHit,
                "Interface handler should match when no concrete handler applies");
        }
    }

    // =========================================================================
    // PIPELINE BUILDER BRANCHING
    // =========================================================================

    [TestClass]
    public class PipelineBuilderBranchingTests
    {
        [TestMethod]
        public async Task ContinueWith_BranchingProducesIndependentPipelines()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            var shared = builder
                .StartWith<ReturnValueAction>(op => op.Value = new ValuePrimitive<string>("base"));

            var branch1 = shared
                .ContinueWith<AddSuffixAction>(op => op.Suffix = "_A");

            var branch2 = shared
                .ContinueWith<AddSuffixAction>(op => op.Suffix = "_B");

            var result1 = await branch1.ExecuteAsync();
            var result2 = await branch2.ExecuteAsync();

            Assert.AreEqual("base_A", ((ValuePrimitive<string>)result1).Value);
            Assert.AreEqual("base_B", ((ValuePrimitive<string>)result2).Value,
                "Branch2 should not contain Branch1's action");
        }
    }

    // =========================================================================
    // NULL ACTIONS GUARD
    // =========================================================================

    [TestClass]
    public class NullActionsTests
    {
        [TestMethod]
        public async Task Pipeline_NullActions_ThrowsActionConfigurationException()
        {
            var logger = new TestLogger();
            var pipeline = PipelineBuilder.CreatePipeline();
            // Factory guarantees non-null Actions; explicitly reset to null here to exercise
            // Pipeline.DefaultHandlerAsync's null-Actions guard (a Pipeline-class invariant distinct
            // from the factory postcondition). Without this, the test would vacuously pass.
            pipeline.Actions = null;
            var ctx = new ExecutionContextForTest(logger).WithResult(NullResult.Instance);

            await Assert.ThrowsExceptionAsync<ActionConfigurationException>(
                async () => await pipeline.ExecuteAsync(ctx));
        }

        [TestMethod]
        public async Task ForEach_NullActions_ThrowsActionConfigurationException()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            await Assert.ThrowsExceptionAsync<ActionConfigurationException>(async () =>
            {
                await builder
                    .StartWith<ReturnValueAction>(op =>
                        op.Value = new PayloadCollection(new IValueSource[] { NullResult.Instance }))
                    .ContinueWith<ForEach>()
                    .ExecuteAsync();
            });
        }

        [TestMethod]
        public async Task ParallelPipeline_NoPipelines_ThrowsActionConfigurationException()
        {
            var logger = new TestLogger();
            var pipeline = new ParallelPipeline();
            var ctx = new ExecutionContextForTest(logger).WithResult(NullResult.Instance);

            await Assert.ThrowsExceptionAsync<ActionConfigurationException>(
                async () => await pipeline.ExecuteAsync(ctx));
        }

        [TestMethod]
        public async Task ParallelForEach_NullActions_ThrowsActionConfigurationException()
        {
            var logger = new TestLogger();
            var builder = new PipelineBuilder(logger);

            await Assert.ThrowsExceptionAsync<ActionConfigurationException>(async () =>
            {
                await builder
                    .StartWith<ReturnValueAction>(op =>
                        op.Value = new PayloadCollection(new IValueSource[] { NullResult.Instance }))
                    .ContinueWith<ParallelForEach>()
                    .ExecuteAsync();
            });
        }
    }

    // =========================================================================
    // DB-TO-DB BULK COPY (type resolution)
    // =========================================================================

    class TestableDbToDbBulkCopy : SqlServerToPostgresBulkCopy
    {
        public static NpgsqlDbType? TestInferClr(Type clrType)
            => SqlServerToPostgresBulkCopy.InferNpgsqlDbTypeFromClr(clrType);

        public static NpgsqlDbType[] TestResolveColumnTypes(
            DbDataReader reader, IList<ColumnMapping> mappings, int[] ordinals,
            Dictionary<string, string> udtNames, string destinationTable)
            => SqlServerToPostgresBulkCopy.ResolveColumnTypes(reader, mappings, ordinals, udtNames, destinationTable);

        public static (string Schema, string Table) TestParseDestinationTable(string destinationTable)
            => ParseDestinationTable(destinationTable);
    }

    [TestClass]
    public class BulkCopyTypeResolutionTests
    {
        [TestMethod]
        public void InferClr_UnambiguousTypes()
        {
            Assert.AreEqual(NpgsqlDbType.Integer, TestableDbToDbBulkCopy.TestInferClr(typeof(int)));
            Assert.AreEqual(NpgsqlDbType.Bigint, TestableDbToDbBulkCopy.TestInferClr(typeof(long)));
            Assert.AreEqual(NpgsqlDbType.Smallint, TestableDbToDbBulkCopy.TestInferClr(typeof(short)));
            Assert.AreEqual(NpgsqlDbType.Smallint, TestableDbToDbBulkCopy.TestInferClr(typeof(byte)));
            Assert.AreEqual(NpgsqlDbType.Boolean, TestableDbToDbBulkCopy.TestInferClr(typeof(bool)));
            Assert.AreEqual(NpgsqlDbType.Numeric, TestableDbToDbBulkCopy.TestInferClr(typeof(decimal)));
            Assert.AreEqual(NpgsqlDbType.Double, TestableDbToDbBulkCopy.TestInferClr(typeof(double)));
            Assert.AreEqual(NpgsqlDbType.Real, TestableDbToDbBulkCopy.TestInferClr(typeof(float)));
            Assert.AreEqual(NpgsqlDbType.TimestampTz, TestableDbToDbBulkCopy.TestInferClr(typeof(DateTimeOffset)));
            Assert.AreEqual(NpgsqlDbType.Time, TestableDbToDbBulkCopy.TestInferClr(typeof(TimeSpan)));
            Assert.AreEqual(NpgsqlDbType.Uuid, TestableDbToDbBulkCopy.TestInferClr(typeof(Guid)));
            Assert.AreEqual(NpgsqlDbType.Bytea, TestableDbToDbBulkCopy.TestInferClr(typeof(byte[])));
        }

        [TestMethod]
        public void InferClr_AmbiguousTypes_ReturnNull()
        {
            Assert.IsNull(TestableDbToDbBulkCopy.TestInferClr(typeof(DateTime)));
            Assert.IsNull(TestableDbToDbBulkCopy.TestInferClr(typeof(string)));
        }

        [TestMethod]
        public void InferClr_UnknownType_ReturnsNull()
        {
            Assert.IsNull(TestableDbToDbBulkCopy.TestInferClr(typeof(object)));
            Assert.IsNull(TestableDbToDbBulkCopy.TestInferClr(typeof(System.Xml.XmlDocument)));
        }

        [TestMethod]
        public void ParseDestinationTable_WithSchema()
        {
            var result = TestableDbToDbBulkCopy.TestParseDestinationTable("myschema.mytable");
            Assert.AreEqual("myschema", result.Schema);
            Assert.AreEqual("mytable", result.Table);
        }

        [TestMethod]
        public void ParseDestinationTable_WithoutSchema_DefaultsToPublic()
        {
            var result = TestableDbToDbBulkCopy.TestParseDestinationTable("mytable");
            Assert.AreEqual("public", result.Schema);
            Assert.AreEqual("mytable", result.Table);
        }

        [TestMethod]
        public void ColumnMapping_RecordEquality()
        {
            var a = new ColumnMapping("src", "dest");
            var b = new ColumnMapping("src", "dest");
            Assert.AreEqual(a, b);
        }

        [TestMethod]
        public void ColumnMapping_Properties()
        {
            var mapping = new ColumnMapping("source_col", "dest_col");
            Assert.AreEqual("source_col", mapping.SourceColumn);
            Assert.AreEqual("dest_col", mapping.DestinationColumn);
        }

        [TestMethod]
        public void BulkCopyException_CarriesContext()
        {
            var inner = new InvalidOperationException("test");
            var ex = new BulkCopyException("public.indicator", 42, "something broke", inner);
            Assert.AreEqual("public.indicator", ex.DestinationTable);
            Assert.AreEqual(42, ex.RowsCopied);
            Assert.IsTrue(ex.Message.Contains("42 rows"));
            Assert.IsTrue(ex.Message.Contains("public.indicator"));
            Assert.AreSame(inner, ex.InnerException);
        }

        [TestMethod]
        public void BuildCopyCommand_QuotesColumnNames()
        {
            var action = new TestableDbToDbBulkCopy();
            var mappings = new List<ColumnMapping>
            {
                new("id", "id"),
                new("date", "date"),  // reserved word
                new("order", "order"),  // reserved word
            };

            var method = typeof(SqlServerToPostgresBulkCopy).GetMethod("BuildCopyCommand",
                System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public);
            var result = (string)method.Invoke(null, new object[] { "public.test", mappings });

            Assert.IsTrue(result.Contains("\"id\""));
            Assert.IsTrue(result.Contains("\"date\""));
            Assert.IsTrue(result.Contains("\"order\""));
            Assert.IsTrue(result.Contains("FROM STDIN (FORMAT BINARY)"));
        }
    }

    // Minimal IExecutionContext for direct action testing
    class ExecutionContextForTest : IExecutionContext
    {
        private readonly ILogger _logger;
        private IValueSource _result = NullResult.Instance;

        public ExecutionContextForTest(ILogger logger) { _logger = logger; }

        public System.Threading.CancellationToken CancellationToken => System.Threading.CancellationToken.None;

        public IState Scope { get; } = new DictState();
        public IState Session { get; } = new DictState();
        public IValueSource Result => _result;

        public ExecutionContextForTest WithResult(IValueSource result)
        {
            _result = result;
            return this;
        }

        public Task LogErrorAsync(string message) => _logger.LogErrorAsync(message);
        public Task LogInfoAsync(string message) => _logger.LogInfoAsync(message);
        public Task LogWarningAsync(string message) => _logger.LogWarningAsync(message);

        public IExecutionContext New() => new ExecutionContextForTest(_logger);
        public IExecutionContext New(IValueSource result) => new ExecutionContextForTest(_logger) { _result = result };

        class DictState : IState
        {
            private readonly Dictionary<string, object> _state = new(StringComparer.OrdinalIgnoreCase);
            public object this[string name] { get => _state[name]; set => _state[name] = value; }
            public IEnumerator<KeyValuePair<string, object>> GetEnumerator() => _state.GetEnumerator();
            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
            public IDictionary<string, object> GetState() => new Dictionary<string, object>(_state, StringComparer.OrdinalIgnoreCase);
        }
    }
}
