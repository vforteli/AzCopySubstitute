using Azure.Storage.Files.DataLake;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace AzCopySubstitute
{
    internal class DataLakePathTraverser
    {
        private readonly DataLakeFileSystemClient _dataLakeFileSystemClient;
        private readonly ILogger _logger;


        /// <summary>
        /// DataLakePathTraverser
        /// </summary>
        /// <param name="dataLakeFileSystemClient">Authenticated filesystem client where the search should start</param>
        /// <param name="logger">Logger</param>
        public DataLakePathTraverser(DataLakeFileSystemClient dataLakeFileSystemClient, ILogger<DataLakePathTraverser> logger)
        {
            _dataLakeFileSystemClient = dataLakeFileSystemClient;
            _logger = logger;
        }


        /// <summary>
        /// List paths recursively using multiple thread for top level directories
        /// </summary>
        /// <param name="searchPath">Directory where recursive listing should start</param>
        /// <param name="paths">BlockingCollection where paths will be stored</param>
        /// <param name="cancellationToken"></param>
        /// <returns>Task which completes when all items have been added to the blocking collection</returns>
        public Task ListPathsAsync(string searchPath, BlockingCollection<string> paths, CancellationToken cancellationToken = default) => Task.Run(async () =>
        {
            // todo this should probably allow specifying depth before calling recursively

            var maxThreads = 48;
            var filesCount = 0;
            var listFilesTasks = new ConcurrentDictionary<Guid, Task>();

            try
            {
                using var semaphore = new SemaphoreSlim(maxThreads, maxThreads);
                await foreach (var path in _dataLakeFileSystemClient.GetPathsAsync(searchPath, recursive: false, cancellationToken: cancellationToken).ConfigureAwait(false))
                {
                    if (path.IsDirectory ?? false)
                    {
                        await semaphore.WaitAsync().ConfigureAwait(false);
                        var taskId = Guid.NewGuid();
                        listFilesTasks.TryAdd(taskId, Task.Run(async () =>
                        {
                            try
                            {
                                await foreach (var childPath in _dataLakeFileSystemClient.GetPathsAsync(path.Name, recursive: true, cancellationToken: cancellationToken).ConfigureAwait(false))
                                {
                                    if (!childPath.IsDirectory ?? false)
                                    {
                                        paths.Add(childPath.Name);
                                        var currentCount = Interlocked.Increment(ref filesCount);
                                        if (currentCount % 1000 == 0)
                                        {
                                            _logger.LogInformation($"Found {currentCount} files...");
                                        }
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, "Failed listing files in path {path}", path.Name);
                            }
                            finally
                            {
                                semaphore.Release();
                            }
                        }).ContinueWith((o) => { listFilesTasks.TryRemove(taskId, out _); }));
                    }
                    else
                    {
                        paths.Add(path.Name);
                        Interlocked.Increment(ref filesCount);
                    }
                }

                _logger.LogInformation("Listed top level directories, waiting for sub directory tasks to complete");
                await Task.WhenAll(listFilesTasks.Values).ConfigureAwait(false);
            }
            catch (TaskCanceledException)
            {
                _logger.LogInformation("\n\nStopping list path task");
            }
            finally
            {
                paths.CompleteAdding();
                _logger.LogInformation($"List files done. Found {filesCount} total files");
            }
        });


        /// <summary>
        /// List paths recursively using multiple thread for top level directories
        /// </summary>        
        /// <param name="paths">BlockingCollection where paths will be stored</param>
        /// <param name="maxThreads">Limits the maximum degree of parallellism</param>
        /// <param name="func">Function to be called for each path</param>
        /// <param name="cancellationToken"></param>        
        public Task<(int totalCount, int processedCount, int failedCount)> ConsumePathsAsync(BlockingCollection<string> paths, Func<string, Task<bool>> func, int maxThreads = 16, CancellationToken cancellationToken = default) => Task.Run(async () =>
       {
           _logger.LogInformation($"Started consume tasks with {maxThreads} max threads");
           var processedCount = 0;
           var failedCount = 0;
           var totalcount = 0;

           var tasks = new ConcurrentDictionary<Guid, Task>();
           using var semaphore = new SemaphoreSlim(maxThreads, maxThreads);

           while (paths.TryTake(out var path, -1, cancellationToken))
           {
               await semaphore.WaitAsync(cancellationToken);

               var taskId = Guid.NewGuid();
               tasks.TryAdd(taskId, Task.Run(async () =>
               {
                   try
                   {
                       await func.Invoke(path).ConfigureAwait(false);

                       var currentCount = Interlocked.Increment(ref processedCount);
                       if (currentCount % 1000 == 0)
                       {
                           Console.WriteLine($"Queued {currentCount} copy tasks...");
                       }
                   }
                   catch (Exception ex)
                   {
                       _logger.LogError(ex, "Something went wrong with {path}", path);
                       Interlocked.Increment(ref failedCount);
                   }
                   finally
                   {
                       semaphore.Release();
                       Interlocked.Increment(ref totalcount);
                   }
               }, cancellationToken).ContinueWith(o => tasks.TryRemove(taskId, out _)));
           }

           await Task.WhenAll(tasks.Values);

           return (processedCount, failedCount, totalcount);
       });
    }
}
