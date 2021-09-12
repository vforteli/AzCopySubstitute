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
        /// <param name="overwrite"></param>
        /// <returns></returns>
        static async Task Main(string sourceConnection, string destinationConnection, bool recursive = true, bool overwrite = true, int threads = 16)
        {
            var sourceUri = new Uri(sourceConnection);
            var destinationUri = new Uri(destinationConnection);


            var sourceDatalakeService = new DataLakeServiceClient(sourceUri);
            var destinationDataLakeService = new BlobServiceClient(destinationUri);

            var filesCount = 0;
            var processedCount = 0;
            var failedCount = 0;
            var totalcount = 0;


            var filesystems = sourceDatalakeService.GetFileSystemsAsync();

            using var semaphore = new SemaphoreSlim(threads, threads);

            var stopwatch = Stopwatch.StartNew();
            var tasks = new ConcurrentDictionary<Guid, Task>();
            var copyTasks = new List<Task>();



            await foreach (var filesystem in filesystems)
            {
                var sourceFileSystemClient = sourceDatalakeService.GetFileSystemClient(filesystem.Name);
                var destinationFileSystemClient = destinationDataLakeService.GetBlobContainerClient(filesystem.Name);
                await destinationFileSystemClient.CreateIfNotExistsAsync();

                var sourcePathNames = new BlockingCollection<string>();

                Console.WriteLine("Starting list files task");
                var listFilesTask = Task.Run(async () =>
                {
                    await foreach (var sourcePath in sourceFileSystemClient.GetPathsAsync(recursive: recursive))
                    {
                        if (!sourcePath.IsDirectory ?? false)
                        {
                            sourcePathNames.Add(sourcePath.Name);
                            var currentCount = Interlocked.Increment(ref filesCount);
                            if (currentCount % 10000 == 0)
                            {
                                Console.WriteLine($"Found {currentCount} files...");
                            }
                        }
                    }

                    sourcePathNames.CompleteAdding();
                    Console.WriteLine($"Found {filesCount} total files");
                    Console.WriteLine("List files task done");
                });

                Console.WriteLine("Starting consume tasks");
                var iterateTask = Task.Run(async () =>
                {
                    while (sourcePathNames.TryTake(out var sourcePath, -1))
                    {
                        await semaphore.WaitAsync();

                        var taskId = Guid.NewGuid();
                        tasks.TryAdd(taskId, Task.Run(async () =>
                        {
                            try
                            {
                                var sourceFileClient = sourceFileSystemClient.GetFileClient(sourcePath);
                                var destinationFileClient = destinationFileSystemClient.GetBlobClient(sourcePath);
                                var status = await destinationFileClient.StartCopyFromUriAsync(sourceFileClient.Uri);
                                //copyTasks.Add(status.WaitForCompletionAsync().AsTask());
                                Interlocked.Increment(ref processedCount);
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(ex);
                                Interlocked.Increment(ref failedCount);
                            }
                            finally
                            {
                                semaphore.Release();
                                Interlocked.Increment(ref totalcount);
                                tasks.TryRemove(taskId, out _);
                            }
                        }));
                    }

                    Console.WriteLine("Consume task done");
                });

                await Task.WhenAll(listFilesTask, iterateTask);
                await Task.WhenAll(tasks.Values);
                Console.WriteLine("All copy tasks have been started, waiting for them to complete...");
                await Task.WhenAll(copyTasks);
            }

            Console.WriteLine($"Done, copy took {stopwatch.Elapsed}");
            Console.WriteLine($"Processed: {processedCount}");
            Console.WriteLine($"Failed: {failedCount}");
            Console.WriteLine($"Total: {totalcount}");
        }
    }
}