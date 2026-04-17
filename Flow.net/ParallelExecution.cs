using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Flow
{
    /// <summary>
    /// Internal helper for semaphore-gated parallel execution.
    /// Consumers: ParallelPipeline, ParallelForEach.
    /// Centralizes the concurrency-cap + cancellation + exception-aggregation pattern
    /// so fixes apply once across all callers.
    /// </summary>
    internal static class ParallelExecution
    {
        /// <summary>
        /// Runs <paramref name="work"/> for each item concurrently with a semaphore-gated cap.
        /// All tasks start immediately; the semaphore gates execution up to
        /// <paramref name="maxDegreeOfParallelism"/> at a time.
        /// Respects <paramref name="cancellationToken"/> at semaphore acquire.
        /// On any failure, propagates an AggregateException via Task.WhenAll —
        /// callers wrap it in their domain-specific exception type.
        /// </summary>
        public static async Task<TResult[]> RunWithConcurrencyCapAsync<TInput, TResult>(
            IReadOnlyList<TInput> items,
            int maxDegreeOfParallelism,
            Func<TInput, Task<TResult>> work,
            CancellationToken cancellationToken = default)
        {
            if (items == null) throw new ArgumentNullException(nameof(items));
            if (work == null) throw new ArgumentNullException(nameof(work));
            if (items.Count == 0) return Array.Empty<TResult>();

            int effectiveDop = maxDegreeOfParallelism <= 0
                ? Environment.ProcessorCount
                : maxDegreeOfParallelism;

            using var semaphore = new SemaphoreSlim(effectiveDop);
            var tasks = new Task<TResult>[items.Count];
            for (int i = 0; i < items.Count; i++)
            {
                var item = items[i];
                tasks[i] = RunOne(item);
            }

            var whenAll = Task.WhenAll(tasks);
            try
            {
                return await whenAll;
            }
            catch
            {
                // await unwraps Faulted to the first inner exception and Canceled to
                // OperationCanceledException. For faults, surface the full AggregateException
                // so callers can observe every failure. For cancellation, let the original
                // OperationCanceledException propagate.
                if (whenAll.IsFaulted && whenAll.Exception != null)
                    throw whenAll.Exception;
                throw;
            }

            async Task<TResult> RunOne(TInput input)
            {
                await semaphore.WaitAsync(cancellationToken);
                try { return await work(input); }
                finally { semaphore.Release(); }
            }
        }
    }
}
