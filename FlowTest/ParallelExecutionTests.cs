using Flow;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace FlowTest
{
    [TestClass]
    public class ParallelExecutionTests
    {
        [TestMethod]
        public async Task RunWithConcurrencyCap_EmptyInput_ReturnsEmpty()
        {
            var items = Array.Empty<int>();
            var result = await ParallelExecution.RunWithConcurrencyCapAsync<int, int>(
                items,
                maxDegreeOfParallelism: 4,
                work: x => Task.FromResult(x * 2));

            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Length);
        }

        [TestMethod]
        public async Task RunWithConcurrencyCap_RespectsCap()
        {
            // 10 items, cap 3, 50ms each → at least ~150ms (4 waves), well under sequential 500ms.
            var items = Enumerable.Range(0, 10).ToArray();

            var sw = Stopwatch.StartNew();
            var result = await ParallelExecution.RunWithConcurrencyCapAsync<int, int>(
                items,
                maxDegreeOfParallelism: 3,
                work: async x => { await Task.Delay(50); return x; });
            sw.Stop();

            Assert.AreEqual(10, result.Length);
            Assert.IsTrue(sw.ElapsedMilliseconds >= 150,
                $"With cap=3 and 10x50ms, expected >=150ms, got {sw.ElapsedMilliseconds}ms");
            Assert.IsTrue(sw.ElapsedMilliseconds < 400,
                $"Cap should parallelize — expected <400ms, got {sw.ElapsedMilliseconds}ms");
        }

        [TestMethod]
        public async Task RunWithConcurrencyCap_HonorsCancellation()
        {
            using var cts = new CancellationTokenSource();
            var items = Enumerable.Range(0, 20).ToArray();

            var task = ParallelExecution.RunWithConcurrencyCapAsync<int, int>(
                items,
                maxDegreeOfParallelism: 2,
                work: async x => { await Task.Delay(500, cts.Token); return x; },
                cancellationToken: cts.Token);

            cts.CancelAfter(50);

            Exception caught = null;
            try { await task; }
            catch (Exception ex) { caught = ex; }

            Assert.IsNotNull(caught, "Expected cancellation to throw");
            Assert.IsInstanceOfType(caught, typeof(OperationCanceledException),
                $"Expected OperationCanceledException (or subclass), got {caught.GetType().Name}");
        }

        [TestMethod]
        public async Task RunWithConcurrencyCap_AggregatesFailures()
        {
            var items = new[] { 1, 2, 3 };

            AggregateException caught = null;
            try
            {
                await ParallelExecution.RunWithConcurrencyCapAsync<int, int>(
                    items,
                    maxDegreeOfParallelism: 3,
                    work: async x =>
                    {
                        await Task.Delay(10);
                        if (x == 1) throw new InvalidOperationException("one");
                        if (x == 2) throw new ArgumentException("two");
                        return x;
                    });
            }
            catch (AggregateException agg)
            {
                caught = agg;
            }

            Assert.IsNotNull(caught, "Helper should rethrow AggregateException");
            Assert.AreEqual(2, caught.InnerExceptions.Count);
            Assert.IsTrue(caught.InnerExceptions.Any(e => e is InvalidOperationException && e.Message == "one"));
            Assert.IsTrue(caught.InnerExceptions.Any(e => e is ArgumentException && e.Message == "two"));
        }

        [TestMethod]
        public async Task RunWithConcurrencyCap_ZeroMaxDop_UsesProcessorCount()
        {
            // With maxDop=0 and ProcessorCount>=2 on CI, 4 items at 50ms each should
            // complete well under sequential 200ms.
            var items = Enumerable.Range(0, 4).ToArray();

            var sw = Stopwatch.StartNew();
            var result = await ParallelExecution.RunWithConcurrencyCapAsync<int, int>(
                items,
                maxDegreeOfParallelism: 0,
                work: async x => { await Task.Delay(50); return x; });
            sw.Stop();

            Assert.AreEqual(4, result.Length);
            Assert.IsTrue(sw.ElapsedMilliseconds < 180,
                $"maxDop=0 should fall back to ProcessorCount — 4x50ms expected <180ms, got {sw.ElapsedMilliseconds}ms");
        }

        [TestMethod]
        public async Task RunWithConcurrencyCap_NegativeMaxDop_UsesProcessorCount()
        {
            var items = Enumerable.Range(0, 4).ToArray();

            var sw = Stopwatch.StartNew();
            var result = await ParallelExecution.RunWithConcurrencyCapAsync<int, int>(
                items,
                maxDegreeOfParallelism: -1,
                work: async x => { await Task.Delay(50); return x; });
            sw.Stop();

            Assert.AreEqual(4, result.Length);
            Assert.IsTrue(sw.ElapsedMilliseconds < 180,
                $"Negative maxDop should fall back to ProcessorCount — 4x50ms expected <180ms, got {sw.ElapsedMilliseconds}ms");
        }
    }
}
