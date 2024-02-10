using Flurl.Http;
using Newtonsoft.Json;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Flow.Quandl
{
    public class QuandlDataDownloader : PipelineAction
    {
        private const int MaxDownloadAttemps = 10;

        public string Url { get; set; }

        public string OutFileName { get; set; }

        public string WorkingDirectory { get; set; }

        protected override async Task<IValue> DefaultHandlerAsync(IExecutionContext context, IValue input)
        {
            var attempCounter = 0;
            var workingDirectory = Format(WorkingDirectory, context, NullResult.Instance, this);
            if (Directory.Exists(workingDirectory))
                Directory.CreateDirectory(workingDirectory);
            while (attempCounter < MaxDownloadAttemps)
            {
                try
                {
                    var result = await Url.GetJsonAsync<Rootobject>();

                    await context.LogInfoAsync($"data {result.Download.File.Status}");
                    if (result.Download.File.Status.Equals("generating", StringComparison.OrdinalIgnoreCase))
                    {
                        await Task.Delay(TimeSpan.FromSeconds(10));
                        continue;
                    }
                    return new FilePath(
                        await result.Download.File.Link.DownloadFileAsync(workingDirectory, OutFileName));
                }
                catch (Exception e)
                {
                    attempCounter += 1;
                    await context.LogErrorAsync(
                        $"attempt {attempCounter} of {MaxDownloadAttemps} failed. Waiting 10sec before tryin again. ERROR [{e.Message}]");
                }
            }
            throw new Exception($"download failed! [URL: ${Url}]");
        }

        private class Rootobject
        {
            [JsonProperty("datatable_bulk_download")]
            public DownloadInfo Download { get; set; }
        }

        private class DownloadInfo
        {
            public DownloadFileInfo File { get; set; }
        }

        private class DownloadFileInfo
        {
            public string Link { get; set; }

            public string Status { get; set; }

            [JsonProperty("data_snapshot_time")]
            public string Date { get; set; }
        }
    }
}
