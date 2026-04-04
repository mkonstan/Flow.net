using System;

namespace Flow
{
    public class FlowException : Exception
    {
        public FlowException(string message) : base(message) { }
        public FlowException(string message, Exception innerException) : base(message, innerException) { }
    }

    public class HandlerNotFoundException : FlowException
    {
        public HandlerNotFoundException(string actionName, string inputTypeName)
            : base($"No handler registered for input type '{inputTypeName}' on action '{actionName}'.")
        { }
    }

    public class ActionConfigurationException : FlowException
    {
        public ActionConfigurationException(string actionName, string message)
            : base($"Action '{actionName}' is misconfigured: {message}")
        { }
    }

    public class BulkCopyException : FlowException
    {
        public string DestinationTable { get; }
        public long RowsCopied { get; }

        public BulkCopyException(string destinationTable, long rowsCopied, string message, Exception innerException)
            : base($"Bulk copy to '{destinationTable}' failed after {rowsCopied} rows: {message}", innerException)
        {
            DestinationTable = destinationTable;
            RowsCopied = rowsCopied;
        }
    }
}
