using Azure.Storage.Files.DataLake;
using Microsoft.Extensions.Logging;
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
            var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.AddConsole();
            });
            var logger = loggerFactory.CreateLogger<DataLakePathTraverser>();

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
            var pathTraverser = new DataLakePathTraverser(sourceFileSystemClient, logger);


            Func<string, Task<bool>> func = async (string path) =>
            {
                var sourceFileClient = sourceFileSystemClient.GetFileClient(path);
                var metadata = await sourceFileClient.GetPropertiesAsync(cancellationToken: cancellationTokenSource.Token).ConfigureAwait(false);
                //var destinationFileClient = destinationFileSystemClient.GetBlobClient(path);
                //var status = await destinationFileClient.StartCopyFromUriAsync(sourceFileClient.Uri);

                //if (waitForCopyResult)
                //{
                //    copyTasks.TryAdd(taskId, status.WaitForCompletionAsync().AsTask().ContinueWith((o) => { copyTasks.TryRemove(taskId, out _); }));
                //}
                return true;
            };


            var paths = new BlockingCollection<string>();

            Console.WriteLine("Starting list files task");
            var listFilesTask = pathTraverser.ListPathsAsync("/", paths, cancellationTokenSource.Token);

            Console.WriteLine("Starting consume tasks");

            var consumeTask = pathTraverser.ConsumePathsAsync(paths, func, threads, cancellationTokenSource.Token);


            Console.WriteLine("Waiting for producer and consumer tasks");
            await Task.WhenAll(listFilesTask, consumeTask);

            var (processedCount, failedCount, totalCount) = consumeTask.Result;

            Console.WriteLine($"Done, copy took {stopwatch.Elapsed}");
            Console.WriteLine($"Processed: {processedCount}");
            Console.WriteLine($"Failed: {failedCount}");
            Console.WriteLine($"Total: {totalCount}");
        }
    }
}