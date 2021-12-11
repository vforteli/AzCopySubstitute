using Azure.Storage.Files.DataLake;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace AzCopySubstitute
{
    class Program
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="sourceConnection">Source connection string with sas token</param>
        /// <param name="destinationConnection">Destination connection string with sas token</param>
        /// <param name="recursive"></param>
        /// <param name="threads"></param>
        /// <param name="waitForCopyResult"></param>        
        /// <returns></returns>
        static async Task Main(string sourceConnection, string destinationConnection, bool recursive = true, int threads = 1000, bool waitForCopyResult = false)
        {
            using var cancellationTokenSource = new CancellationTokenSource();

            Console.CancelKeyPress += (s, e) =>
            {
                if (!cancellationTokenSource.IsCancellationRequested)
                {
                    e.Cancel = true;
                    Console.WriteLine("Breaking, waiting for queued tasks to complete. Press break again to force stop");
                    cancellationTokenSource.Cancel();
                }
                else
                {
                    Console.WriteLine("Terminating threads");
                    Environment.Exit(1);
                }
            };

            var stopwatch = Stopwatch.StartNew();


            var sourceDatalakeService = new DataLakeServiceClient(new Uri(sourceConnection));
            var sourceFileSystemClient = sourceDatalakeService.GetFileSystemClient("stuff");
            var rootDirectory = sourceFileSystemClient.GetDirectoryClient("/");

            var paths = new BlockingCollection<string>();

            Console.WriteLine("Starting list files task");
            var listFilesTask = DataLakePathTraverser.ListPathsAsync(rootDirectory, paths, cancellationTokenSource.Token);

            Console.WriteLine("Starting consume tasks");
            var consumeTask = TestConsumer.Consume(paths, sourceFileSystemClient, 1000, cancellationTokenSource.Token);


            Console.WriteLine("Waiting for producer and consumer tasks");
            await Task.WhenAll(listFilesTask, consumeTask);

            var (processedCount, failedCount, totalCount) = consumeTask.Result;

            //Console.WriteLine($"Found {paths.Count} files");
            //if (paths.Count != paths.ToList().Distinct().Count())
            //{
            //    Console.Error.WriteLine("Uh oh, something doesnt add up with paths");
            //    Environment.Exit(1);
            //}

            Console.WriteLine($"Done, copy took {stopwatch.Elapsed}");
            Console.WriteLine($"Processed: {processedCount}");
            Console.WriteLine($"Failed: {failedCount}");
            Console.WriteLine($"Total: {totalCount}");
        }
    }
}