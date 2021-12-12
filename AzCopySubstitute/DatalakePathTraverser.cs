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
    }
}

