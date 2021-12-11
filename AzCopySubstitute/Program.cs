using Azure.Storage.Blobs;
using Azure.Storage.Files.DataLake;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
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
            var sourceUri = new Uri(sourceConnection);

            var sourceDatalakeService = new DataLakeServiceClient(sourceUri);

            var processedCount = 0;
            var failedCount = 0;
            var totalcount = 0;
            using var cancellationTokenSource = new CancellationTokenSource();


            using var semaphore = new SemaphoreSlim(threads, threads);

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



            var sourceFileSystemClient = sourceDatalakeService.GetFileSystemClient("stuff");
            var rootDirectory = sourceFileSystemClient.GetDirectoryClient("/");


            var sourcePathNames = new BlockingCollection<string>();



            Console.WriteLine("Starting list files task");
            var listFilesTask = DataLakePathTraverser.ListPathsAsync(rootDirectory, sourcePathNames, cancellationTokenSource.Token);



            Console.WriteLine("Waiting for list files and iterate tasks");
            await Task.WhenAll(listFilesTask);


            Console.WriteLine(sourcePathNames.Count);
            Console.WriteLine($"Done, copy took {stopwatch.Elapsed}");
            Console.WriteLine($"Processed: {processedCount}");
            Console.WriteLine($"Failed: {failedCount}");
            Console.WriteLine($"Total: {totalcount}");
        }
    }
}