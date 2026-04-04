using System;
using System.Threading.Tasks;

namespace Flow.Data.ETL
{
    public sealed class TransferContext
    {
        public IExecutionContext ExecutionContext { get; set; }
        public string DestinationTable { get; set; }
        public int BatchSize { get; set; }
        public int NotifyAfter { get; set; }
        public Func<IExecutionContext, long, Task> OnBatchComplete { get; set; }
    }
}
