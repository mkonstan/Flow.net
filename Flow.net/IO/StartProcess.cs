using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Flow.IO
{
	public class StartProcess : PipelineAction
	{
		public string FileName { get; set; }
		public string Arguments { get; set; }
		public TimeSpan? Timeout { get; set; }

		protected override async Task<IPayload> DefaultHandlerAsync(IExecutionContext context, IPayload input)
		{
			var filePathAndName = Format(FileName, context, NullResult.Instance, this);
			var arguments = Format(Arguments, context, NullResult.Instance, this);
			var fileName = Path.GetFileName(filePathAndName);
			var workingDirectory = Path.GetDirectoryName(filePathAndName);
			var timeout = (int)(Timeout.HasValue ? Timeout.Value.TotalMilliseconds : -1);
			var processInfo = new ProcessStartInfo(fileName)
			{
				FileName = filePathAndName,
				WorkingDirectory = workingDirectory,
				Arguments = arguments,
			};
			try
			{
				using (var proc = Process.Start(processInfo))
				{
					if (!proc.WaitForExit(timeout))
					{
						throw new Exception($"Process timeout expired! Timeout {Timeout.Value.TotalMinutes} min.");

					}
					return await Task.FromResult(new ValueResult<int>(proc.ExitCode));
				}
			}
			catch (Exception e)
			{
				context.LogError(
					$"ERROR: Unable to start process {FileName} with Arguments: {Arguments}. ERROR [{e.Message}]");
				throw;
			}
		}
	}
}
