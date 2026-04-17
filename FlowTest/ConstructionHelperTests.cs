using Flow;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace FlowTest
{
    [TestClass]
    public class ConstructionHelperTests
    {
        // ===== PipelineBuilder.CreatePipeline() =====

        [TestMethod]
        public void CreatePipeline_NoArgs_ReturnsEmpty()
        {
            var pipeline = PipelineBuilder.CreatePipeline();
            Assert.IsNotNull(pipeline);
            Assert.IsNotNull(pipeline.Actions);
            Assert.AreEqual(0, pipeline.Actions.Count());
        }

        [TestMethod]
        public void CreatePipeline_Params_WithActions()
        {
            var a = new CaptureAction();
            var b = new CaptureAction();
            var pipeline = PipelineBuilder.CreatePipeline(a, b);

            var actions = pipeline.Actions.ToArray();
            Assert.AreEqual(2, actions.Length);
            Assert.AreSame(a, actions[0]);
            Assert.AreSame(b, actions[1]);
        }

        [TestMethod]
        public void CreatePipeline_Params_Null_ReturnsEmpty()
        {
            var pipeline = PipelineBuilder.CreatePipeline((IPipelineAction[])null);
            Assert.IsNotNull(pipeline);
            Assert.IsNotNull(pipeline.Actions);
            Assert.AreEqual(0, pipeline.Actions.Count());
        }

        [TestMethod]
        public void CreatePipeline_Params_Empty_ReturnsEmpty()
        {
            var pipeline = PipelineBuilder.CreatePipeline(Array.Empty<IPipelineAction>());
            Assert.IsNotNull(pipeline);
            Assert.IsNotNull(pipeline.Actions);
            Assert.AreEqual(0, pipeline.Actions.Count());
        }

        [TestMethod]
        public void CreatePipeline_ActionBody_ConfiguresPipeline()
        {
            var pipeline = PipelineBuilder.CreatePipeline(p =>
                p.AddAction<CaptureAction>(_ => { })
                 .AddAction<CaptureAction>(_ => { }));

            Assert.AreEqual(2, pipeline.Actions.Count());
            Assert.IsTrue(pipeline.Actions.All(a => a is CaptureAction));
        }

        [TestMethod]
        public void CreatePipeline_ActionBody_NullBody_Throws()
        {
            Assert.ThrowsException<ArgumentNullException>(
                () => PipelineBuilder.CreatePipeline((Action<IPipeline>)null));
        }

        [TestMethod]
        public void CreatePipeline_GenericT_CreatesSingleActionPipeline()
        {
            var pipeline = PipelineBuilder.CreatePipeline<ReturnConstantAction>(
                a => a.Value = new ValuePrimitive<string>("x"));

            var actions = pipeline.Actions.ToArray();
            Assert.AreEqual(1, actions.Length);
            Assert.IsInstanceOfType(actions[0], typeof(ReturnConstantAction));
            var value = ((ReturnConstantAction)actions[0]).Value as ValuePrimitive<string>;
            Assert.IsNotNull(value);
            Assert.AreEqual("x", value.Value);
        }

        [TestMethod]
        public void CreatePipeline_GenericT_NullBody_Throws()
        {
            Assert.ThrowsException<ArgumentNullException>(
                () => PipelineBuilder.CreatePipeline<CaptureAction>(null));
        }

        // ===== ParallelPipelineHelpers.AddPipeline(Action<IPipeline>) =====

        [TestMethod]
        public void AddPipeline_ActionBody_AppendsBranch()
        {
            var parallel = new ParallelPipeline();
            parallel.AddPipeline(p => p.AddAction<CaptureAction>(_ => { }));

            Assert.AreEqual(1, parallel.Pipelines.Count());
            var branch = parallel.Pipelines.First();
            Assert.AreEqual(1, branch.Actions.Count());
            Assert.IsInstanceOfType(branch.Actions.First(), typeof(CaptureAction));
        }

        [TestMethod]
        public void AddPipeline_ActionBody_NullBody_Throws()
        {
            var parallel = new ParallelPipeline();
            Assert.ThrowsException<ArgumentNullException>(
                () => parallel.AddPipeline((Action<IPipeline>)null));
        }

        // ===== ParallelPipelineHelpers.AddPipeline<T>(Action<T>) =====

        [TestMethod]
        public void AddPipeline_GenericT_AppendsSingleActionBranch()
        {
            var parallel = new ParallelPipeline();
            parallel.AddPipeline<ReturnConstantAction>(
                a => a.Value = new ValuePrimitive<string>("metric"));

            Assert.AreEqual(1, parallel.Pipelines.Count());
            var branch = parallel.Pipelines.First();
            Assert.AreEqual(1, branch.Actions.Count());
            Assert.IsInstanceOfType(branch.Actions.First(), typeof(ReturnConstantAction));
        }

        [TestMethod]
        public void AddPipeline_GenericT_NullBody_Throws()
        {
            var parallel = new ParallelPipeline();
            Assert.ThrowsException<ArgumentNullException>(
                () => parallel.AddPipeline<CaptureAction>(null));
        }

        // ===== Chainability + integration =====

        [TestMethod]
        public void AddPipeline_Fluent_Chains()
        {
            var parallel = new ParallelPipeline();
            var returned = parallel
                .AddPipeline(p => p.AddAction<CaptureAction>(_ => { }))
                .AddPipeline<CaptureAction>(_ => { });

            Assert.AreSame(parallel, returned);
            Assert.AreEqual(2, parallel.Pipelines.Count());
        }

        [TestMethod]
        public async Task AddPipeline_GenericT_IntegrationWithParallelPipeline()
        {
            var parallel = new ParallelPipeline()
                .AddPipeline<CaptureAction>(_ => { })
                .AddPipeline<CaptureAction>(_ => { });

            var logger = new TestLogger();
            var ctx = new ExecutionContextForTest(logger)
                .WithResult(new ValuePrimitive<string>("input"));

            var result = await parallel.ExecuteAsync(ctx);
            Assert.IsInstanceOfType(result, typeof(PayloadCollection));
            var branches = parallel.Pipelines.ToArray();
            foreach (var branch in branches)
            {
                var capture = (CaptureAction)branch.Actions.First();
                Assert.IsTrue(capture.WasCalled);
            }
        }

        // ===== Honesty test: factory normalizes post-callback =====

        [TestMethod]
        public void CreatePipeline_CallbackReassignsActionsToNull_NormalizesToEmpty()
        {
            var pipeline = PipelineBuilder.CreatePipeline(p =>
            {
                p.AddAction<CaptureAction>(_ => { });
                p.Actions = null;
            });

            Assert.IsNotNull(pipeline);
            Assert.IsNotNull(pipeline.Actions, "Factory must normalize null Actions to empty array.");
            Assert.AreEqual(0, pipeline.Actions.Count());
        }
    }

}
