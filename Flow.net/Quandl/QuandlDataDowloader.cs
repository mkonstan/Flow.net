using Flurl.Http;
using System;
using System.IO;
using System.Linq;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Flow.Quandl
{
    public class QuandlDataDownloader : PipelineAction
    {
        private const int MaxDownloadAttemps = 10;

        public string Url { get; set; }

        public string OutFileName { get; set; }

        public string WorkingDirectory { get; set; }

        protected override async Task<IValueSource> DefaultHandlerAsync(IExecutionContext context, IValueSource input)
        {
            var attempCounter = 0;
            var workingDirectory = Format(WorkingDirectory, context, NullResult.Instance, this);
            if (!Directory.Exists(workingDirectory))
                Directory.CreateDirectory(workingDirectory);
            Rootobject result = null;
            while (attempCounter < MaxDownloadAttemps)
            {
                try
                {
                    result = await Url.GetJsonAsync<Rootobject>();

                    if (result?.Download?.File == null)
                    {
                        throw new FlowException(
                            $"Unexpected API response — 'datatable_bulk_download.file' is missing. Raw response may indicate an API change or invalid key.");
                    }

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
                    var responseState = result != null
                        ? new { Download = result.Download != null, File = result.Download?.File != null, Status = result.Download?.File?.Status, Link = result.Download?.File?.Link }.Serialize()
                        : "null";
                    await context.LogErrorAsync(
                        $"attempt {attempCounter} of {MaxDownloadAttemps} failed. Waiting 10sec before trying again.\n" +
                        $"  URL: {Url}\n" +
                        $"  WorkingDirectory: {workingDirectory}\n" +
                        $"  OutFileName: {OutFileName}\n" +
                        $"  Response: {responseState}\n" +
                        $"  ERROR: {e}");
                    await Task.Delay(TimeSpan.FromSeconds(10));
                }
            }
            throw new Exception($"download failed! [URL: ${Url}]");
        }

        private class Rootobject
        {
            [JsonPropertyName("datatable_bulk_download")]
            public DownloadInfo Download { get; set; }
        }

        private class DownloadInfo
        {
            [JsonPropertyName("file")]
            public DownloadFileInfo File { get; set; }
        }

        private class DownloadFileInfo
        {
            [JsonPropertyName("link")]
            public string Link { get; set; }

            [JsonPropertyName("status")]
            public string Status { get; set; }

            [JsonPropertyName("data_snapshot_time")]
            public string Date { get; set; }
        }
    }
}
