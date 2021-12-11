using Azure.Storage.Files.DataLake;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace AzCopySubstitute
{
    internal class TestConsumer
    {
        public static Task<(int processedCount, int failedCount, int totalCount)> Consume(BlockingCollection<string> paths, DataLakeFileSystemClient sourceFileSystemClient, int threads, CancellationToken cancellationToken = default) => Task.Run<(int processedCount, int failedCount, int totalCound)>(async () =>
        {
            var processedCount = 0;
            var failedCount = 0;
            var totalcount = 0;
            //return (processedCount, failedCount, totalcount);
            var consumeTasks = new ConcurrentDictionary<Guid, Task>();
            using var semaphore = new SemaphoreSlim(threads, threads);

            while (paths.TryTake(out var path, -1, cancellationToken))
            {
                await semaphore.WaitAsync(cancellationToken);

                var taskId = Guid.NewGuid();
                consumeTasks.TryAdd(taskId, Task.Run(async () =>
                {
                    try
                    {
                        var sourceFileClient = sourceFileSystemClient.GetFileClient(path);
                        var metadata = await sourceFileClient.GetPropertiesAsync(cancellationToken: cancellationToken);
                        //var destinationFileClient = destinationFileSystemClient.GetBlobClient(path);
                        //var status = await destinationFileClient.StartCopyFromUriAsync(sourceFileClient.Uri);

                        //if (waitForCopyResult)
                        //{
                        //    copyTasks.TryAdd(taskId, status.WaitForCompletionAsync().AsTask().ContinueWith((o) => { copyTasks.TryRemove(taskId, out _); }));
                        //}

                        var currentCount = Interlocked.Increment(ref processedCount);
                        if (currentCount % 10000 == 0)
                        {
                            Console.WriteLine($"Queued {currentCount} copy tasks...");
                        }
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
                        consumeTasks.TryRemove(taskId, out _);
                    }
                }, cancellationToken));
            }

            await Task.WhenAll(consumeTasks.Values);

            return (processedCount, failedCount, totalcount);
        });
    }
}
