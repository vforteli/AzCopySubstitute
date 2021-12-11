using Azure.Storage.Files.DataLake;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace AzCopySubstitute
{
    internal class DataLakePathTraverser
    {
        /// <summary>
        /// List paths recursively using multiple thread for top level directories
        /// </summary>
        /// <param name="directoryClient">Directory where recursive listing should start</param>
        /// <param name="paths">BlockingCollection where paths will be stored</param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static Task ListPathsAsync(DataLakeDirectoryClient directoryClient, BlockingCollection<string> paths, CancellationToken cancellationToken = default) => Task.Run(async () =>
        {
            // todo this should probably allow specifying depth before calling recursively

            var maxThreads = 16;
            var filesCount = 0;
            var listFilesTasks = new ConcurrentDictionary<Guid, Task>();

            try
            {
                using var semaphore = new SemaphoreSlim(maxThreads, maxThreads);
                await foreach (var path in directoryClient.GetPathsAsync(recursive: false, cancellationToken: cancellationToken))
                {
                    // todo this should also handle files in root directories
                    if (path.IsDirectory ?? false)
                    {
                        await semaphore.WaitAsync();
                        var taskId = Guid.NewGuid();
                        listFilesTasks.TryAdd(taskId, Task.Run(async () =>
                        {
                            try
                            {
                                await foreach (var childPath in directoryClient.GetSubDirectoryClient(path.Name).GetPathsAsync(recursive: true, cancellationToken: cancellationToken))
                                {
                                    if (!childPath.IsDirectory ?? false)
                                    {
                                        paths.Add(childPath.Name);
                                        var currentCount = Interlocked.Increment(ref filesCount);
                                        if (currentCount % 1000 == 0)
                                        {
                                            Console.WriteLine($"Found {currentCount} files...");
                                        }
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("Failed listing files in path {path}", path.Name);
                                Console.WriteLine(ex);
                            }
                            finally
                            {
                                semaphore.Release();
                            }
                        }).ContinueWith((o) => { listFilesTasks.TryRemove(taskId, out _); }));
                    }
                }

                Console.WriteLine("Listed top level directories, waiting for sub directory tasks to complete");
                await Task.WhenAll(listFilesTasks.Values);
            }
            catch (TaskCanceledException)
            {
                Console.WriteLine("\n\nStopping list path task");
            }
            finally
            {
                paths.CompleteAdding();
                Console.WriteLine($"List files done. Found {filesCount} total files");
            }
        });
    }
}

