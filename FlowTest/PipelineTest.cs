using Flow;
using Flow.Data.SqlServer;
using Flow.IO;
using Flow.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using System;
using System.IO;
using System.Diagnostics;
using System.Threading.Tasks;

namespace FlowTest
{
    public static class Config
    {
        public static string ProjectDir => Directory.GetParent(Environment.CurrentDirectory).Parent.Parent.FullName;
        public static string TestInputDir => Path.Combine(new string[] { ProjectDir, "TestMaterial", "Input" });
        public static string TestTargetDir => Path.Combine(new string[] { ProjectDir, "TestMaterial", "Output" });
    }


    [TestClass]
    public class PipelineTest
    {
        protected static readonly ILogger Logger = new TraceLogger();

        [TestMethod]
        public async Task FileScopingTest()
        {
            var builder = new PipelineBuilder(Logger);
            var result = await builder.StartWith<GetFiles>(op =>
                {
                    op.DirectoryPath = Config.TestInputDir;
                    op.SearchPattern = "*.csv";
                    op.SearchOption = System.IO.SearchOption.AllDirectories;
                })
                .ContinueWith<StoreInScope>(op =>
                {
                    op.Name = "FileCollection";
                })
                .ContinueWith<LogContext>()
                .ContinueWith<ForEach>(op =>
                {
                    op.PayloadProvider = new GetPayloadFromScope { Name = "FileCollection" };
                    op.Actions = builder.StartWith<LogResult>().Create().Actions;
                })
                .ExecuteAsync();

            Trace.WriteLine(JsonConvert.SerializeObject(result, Formatting.Indented));
            Assert.IsTrue(result is PayloadCollection);
        }



        [TestMethod]
        public async Task CopyFilesTest()
        {
            // Confirm files exist in test input dir
            string testFileIn1 = Path.Combine(new string[] { Config.TestInputDir, "sample_data_1.csv" });
            string testFileIn2 = Path.Combine(new string[] { Config.TestInputDir, "sample_data_2.csv" });

            Assert.IsTrue(File.Exists(testFileIn1));
            Assert.IsTrue(File.Exists(testFileIn2));

            // Confirm files don't exist in test output dir
            string testFileOut1 = Path.Combine(new string[] { Config.TestInputDir, "sample_data_1.csv" });
            string testFileOut2 = Path.Combine(new string[] { Config.TestInputDir, "sample_data_2.csv" });

            if (File.Exists(testFileOut1)) { File.Delete(testFileOut1); }
            if (File.Exists(testFileOut2)) { File.Delete(testFileOut2); }

            Assert.IsFalse(File.Exists(testFileOut1));
            Assert.IsFalse(File.Exists(testFileOut2));


            // Test Copy Files
            var builder = new PipelineBuilder(Logger);
            var result = await builder.StartWith<GetFiles>(op =>
                {
                    op.DirectoryPath = Config.TestInputDir;
                    op.SearchPattern = "*.csv";
                    op.SearchOption = System.IO.SearchOption.AllDirectories;
                })
                .ContinueWith<CopyFiles>(op =>
                {
                    op.DirectoryPath = Config.TestTargetDir;
                })
                .ExecuteAsync();

            Trace.WriteLine(JsonConvert.SerializeObject(result, Formatting.Indented));
            Assert.IsTrue(result is PayloadCollection);


            // Confirm files now exist in test output dir
            Assert.IsTrue(File.Exists(testFileOut1));
            Assert.IsTrue(File.Exists(testFileOut2));

        }

        [TestMethod]
        public async Task MoveFilesTest()
        {


            // Confirm files exist in test input dir
            string testFileIn1 = Path.Combine(new string[] { Config.TestInputDir, "sample_data_1.csv" });
            string testFileIn2 = Path.Combine(new string[] { Config.TestInputDir, "sample_data_2.csv" });

            Assert.IsTrue(File.Exists(testFileIn1));
            Assert.IsTrue(File.Exists(testFileIn2));

            // Confirm files don't exist in test output dir
            string testFileOut1 = Path.Combine(new string[] { Config.TestInputDir, "sample_data_1.csv" });
            string testFileOut2 = Path.Combine(new string[] { Config.TestInputDir, "sample_data_2.csv" });

            if (File.Exists(testFileOut1)) { File.Delete(testFileOut1); }
            if (File.Exists(testFileOut2)) { File.Delete(testFileOut2); }

            Assert.IsFalse(File.Exists(testFileOut1));
            Assert.IsFalse(File.Exists(testFileOut2));


            // Test Move Files
            var builder = new PipelineBuilder(Logger);
            var result = await builder.StartWith<GetFiles>(op =>
                {
                    op.DirectoryPath = Config.TestInputDir;
                    op.SearchPattern = "*.csv";
                    op.SearchOption = System.IO.SearchOption.AllDirectories;
                })
                .ContinueWith<MoveFiles>(op =>
                {
                    op.DirectoryPath = Config.TestTargetDir;
                })
                .ExecuteAsync();

            Trace.WriteLine(JsonConvert.SerializeObject(result, Formatting.Indented));
            Assert.IsTrue(result is PayloadCollection);


            // Confirm files now exist in test output dir
            Assert.IsTrue(File.Exists(testFileOut1));
            Assert.IsTrue(File.Exists(testFileOut2));

            // Confirm files now don't exist in test input dir
            Assert.IsFalse(File.Exists(testFileIn1));
            Assert.IsFalse(File.Exists(testFileIn2));


            // Move files back
            var builderClean = new PipelineBuilder(Logger);
            var resultClean = await builder.StartWith<GetFiles>(op =>
                {
                    op.DirectoryPath = Config.TestTargetDir;
                    op.SearchPattern = "*.csv";
                    op.SearchOption = System.IO.SearchOption.AllDirectories;
                })
                .ContinueWith<MoveFiles>(op =>
                {
                    op.DirectoryPath = Config.TestInputDir;
                })
                .ExecuteAsync();


            // Confirm files now exist in test input dir
            Assert.IsTrue(File.Exists(testFileIn1));
            Assert.IsTrue(File.Exists(testFileIn2));

            // Confirm files now don't exist in test output dir
            Assert.IsFalse(File.Exists(testFileOut1));
            Assert.IsFalse(File.Exists(testFileOut2));

        }



        [TestMethod]
        public async Task DeleteFilesTest()
        {


            // Confirm files exist in test input dir
            string testFileIn1 = Path.Combine(new string[] { Config.TestInputDir, "sample_data_1.csv" });
            string testFileIn2 = Path.Combine(new string[] { Config.TestInputDir, "sample_data_2.csv" });

            Assert.IsTrue(File.Exists(testFileIn1));
            Assert.IsTrue(File.Exists(testFileIn2));

            // Confirm files don't exist in test output dir
            string testFileOut1 = Path.Combine(new string[] { Config.TestInputDir, "sample_data_1.csv" });
            string testFileOut2 = Path.Combine(new string[] { Config.TestInputDir, "sample_data_2.csv" });

            if (File.Exists(testFileOut1)) { File.Delete(testFileOut1); }
            if (File.Exists(testFileOut2)) { File.Delete(testFileOut2); }

            Assert.IsFalse(File.Exists(testFileOut1));
            Assert.IsFalse(File.Exists(testFileOut2));


            // Copy Files
            var builder = new PipelineBuilder(Logger);
            var copyFilesForDelete = await builder.StartWith<GetFiles>(op =>
                {
                    op.DirectoryPath = Config.TestInputDir;
                    op.SearchPattern = "*.csv";
                    op.SearchOption = System.IO.SearchOption.AllDirectories;
                })
                .ContinueWith<CopyFiles>(op =>
                {
                    op.DirectoryPath = Config.TestTargetDir;
                })
                .ExecuteAsync();

            // Confirm files to delete exist
            Assert.IsTrue(File.Exists(testFileOut1));
            Assert.IsTrue(File.Exists(testFileOut2));


            // Test Delete Files
            var result = await builder.StartWith<GetFiles>(op =>
                {
                    op.DirectoryPath = Config.TestInputDir;
                    op.SearchPattern = "*.csv";
                    op.SearchOption = System.IO.SearchOption.AllDirectories;
                })
                .ContinueWith<DeleteFiles>()
                .ExecuteAsync();

            Trace.WriteLine(JsonConvert.SerializeObject(result, Formatting.Indented));
            Assert.IsTrue(result is NullResult);


            // Confirm files were deleted
            Assert.IsFalse(File.Exists(testFileOut1));
            Assert.IsFalse(File.Exists(testFileOut2));



        }


    }
}