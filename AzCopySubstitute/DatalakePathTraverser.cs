using Azure.Storage.Files.DataLake;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace AzCopySubstitute
{
    internal class DataLakePathTraverser
    {
        private readonly ILogger _logger;


        /// <summary>
        /// DataLakePathTraverser
        /// </summary>        
        /// <param name="logger">Logger</param>
        public DataLakePathTraverser(ILogger<DataLakePathTraverser> logger)
        {
            _logger = logger;
        }


        /// <summary>
        /// List paths recursively using multiple thread for top level directories
        /// </summary>
        /// <param name="dataLakeFileSystemClient">Authenticated filesystem client where the search should start</param>
        /// <param name="searchPath">Directory where recursive listing should start</param>
        /// <param name="paths">BlockingCollection where paths will be stored</param>
        /// <param name="maxThreads">Max degrees of parallelism</param>
        /// <param name="cancellationToken"></param>
        /// <returns>Task which completes when all items have been added to the blocking collection</returns>
        public Task ListPathsAsync(DataLakeFileSystemClient dataLakeFileSystemClient, string searchPath, BlockingCollection<string> paths, int maxThreads = 16, CancellationToken cancellationToken = default) => Task.Run(async () =>
        {
            var filesCount = 0;
            var tasks = new ConcurrentDictionary<Guid, Task>();
            var sw = Stopwatch.StartNew();

            try
            {
                using var semaphore = new SemaphoreSlim(maxThreads, maxThreads);
                await foreach (var path in dataLakeFileSystemClient.GetPathsAsync(searchPath, recursive: false, cancellationToken: cancellationToken).ConfigureAwait(false))
                {
                    if (path.IsDirectory ?? false)
                    {
                        await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
                        var taskId = Guid.NewGuid();
                        tasks.TryAdd(taskId, Task.Run(async () =>
                        {
                            try
                            {
                                await foreach (var childPath in dataLakeFileSystemClient.GetPathsAsync(path.Name, recursive: true, cancellationToken: cancellationToken).ConfigureAwait(false))
                                {
                                    if (!childPath.IsDirectory ?? false)
                                    {
                                        paths.Add(childPath.Name);
                                        var currentCount = Interlocked.Increment(ref filesCount);
                                        if (currentCount % 1000 == 0)
                                        {
                                            _logger.LogInformation($"Found {currentCount} files. {currentCount / (sw.ElapsedMilliseconds / 1000)} fps");
                                        }
                                    }
                                }
                            }
                            catch (TaskCanceledException) { }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, "Failed listing files in path {path}", path.Name);
                            }
                            finally
                            {
                                semaphore.Release();
                            }
                        }, cancellationToken).ContinueWith((o) => { tasks.TryRemove(taskId, out _); }));
                    }
                    else
                    {
                        paths.Add(path.Name);
                        Interlocked.Increment(ref filesCount);
                    }
                }

                _logger.LogInformation("Listed top level directories, waiting for sub directory tasks to complete");
                await Task.WhenAll(tasks.Values).ConfigureAwait(false);
            }
            catch (TaskCanceledException)
            {
                _logger.LogWarning("Cancelled list path task");
            }
            finally
            {
                paths.CompleteAdding();
                _logger.LogInformation($"List files done. Found {filesCount} total files. {filesCount / (sw.ElapsedMilliseconds / 1000f)} fps, duration: {sw.ElapsedMilliseconds}ms");
            }
        });


        /// <summary>
        /// List paths recursively using multiple thread for top level directories
        /// </summary>        
        /// <param name="paths">BlockingCollection where paths will be stored</param>
        /// <param name="maxThreads">Max degrees of parallelism</param>
        /// <param name="func">Function to be called for each path</param>
        /// <param name="cancellationToken"></param>        
        public Task<(int totalCount, int processedCount, int failedCount)> ConsumePathsAsync(BlockingCollection<string> paths, Func<string, Task<bool>> func, int maxThreads = 16, CancellationToken cancellationToken = default) => Task.Run(async () =>
        {
            var processedCount = 0;
            var failedCount = 0;
            var totalCount = 0;

            var sw = Stopwatch.StartNew();
            var tasks = new ConcurrentDictionary<Guid, Task>();
            using var semaphore = new SemaphoreSlim(maxThreads, maxThreads);

            while (paths.TryTake(out var path, -1, cancellationToken))
            {
                await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

                var taskId = Guid.NewGuid();
                tasks.TryAdd(taskId, Task.Run(async () =>
                {
                    try
                    {
                        if (await func.Invoke(path).ConfigureAwait(false))
                        {
                            var currentCount = Interlocked.Increment(ref processedCount);
                            if (currentCount % 1000 == 0)
                            {
                                _logger.LogInformation($"Processed {currentCount} files... {totalCount / (sw.ElapsedMilliseconds / 1000f)} fps");
                            }
                        }
                    }
                    catch (TaskCanceledException) { }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Something went wrong with {path}", path);
                        Interlocked.Increment(ref failedCount);
                    }
                    finally
                    {
                        semaphore.Release();
                        Interlocked.Increment(ref totalCount);
                    }
                }, cancellationToken).ContinueWith(o => tasks.TryRemove(taskId, out _)));
            }

            await Task.WhenAll(tasks.Values).ConfigureAwait(false);
            _logger.LogInformation($"{totalCount / (sw.ElapsedMilliseconds / 1000f)} fps");

            return (processedCount, failedCount, totalCount);
        });
    }
}
