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
        /// <param name="listThreads"></param>
        /// <param name="workerThreads"></param>       
        /// <param name="waitForCopyResult"></param>        
        /// <returns></returns>
        static async Task Main(string sourceConnection, string destinationConnection, bool recursive = true, int listThreads = 16, int workerThreads = 16, bool waitForCopyResult = false)
        {
            var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.AddSimpleConsole(o =>
                {
                    o.SingleLine = true;
                });
            });
            var logger = loggerFactory.CreateLogger<DataLakePathTraverser>();

            using var cancellationTokenSource = new CancellationTokenSource();

            Console.CancelKeyPress += (s, e) =>
            {
                if (!cancellationTokenSource.IsCancellationRequested)
                {
                    e.Cancel = true;
                    logger.LogWarning("Breaking, waiting for queued tasks to complete. Press break again to force stop");
                    cancellationTokenSource.Cancel();
                }
                else
                {
                    logger.LogWarning("Terminating threads");
                    Environment.Exit(1);
                }
            };

            var stopwatch = Stopwatch.StartNew();

            var sourceFileSystemClient = new DataLakeServiceClient(new Uri(sourceConnection)).GetFileSystemClient("stuff");
            var pathTraverser = new DataLakePathTraverser(logger);


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

            logger.LogInformation("Starting list files task");
            var listFilesTask = pathTraverser.ListPathsAsync(sourceFileSystemClient, "/", paths, listThreads, cancellationTokenSource.Token);

            logger.LogInformation("Starting consume tasks");
            var consumeTask = pathTraverser.ConsumePathsAsync(paths, func, workerThreads, cancellationTokenSource.Token);

            logger.LogInformation("Waiting for producer and consumer tasks");
            await Task.WhenAll(listFilesTask, consumeTask);

            var (processedCount, failedCount, totalCount) = consumeTask.Result;

            logger.LogInformation($"Done, took {stopwatch.Elapsed}");
            logger.LogInformation($"Processed: {processedCount}");
            logger.LogInformation($"Failed: {failedCount}");
            logger.LogInformation($"Total: {totalCount}");
        }
    }
}