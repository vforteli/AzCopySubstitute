﻿using Azure.Storage.Blobs;
using Azure.Storage.Files.DataLake;
using System;
using System.Collections.Concurrent;
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
            var destinationUri = new Uri(destinationConnection);

            var sourceDatalakeService = new DataLakeServiceClient(sourceUri);
            var destinationDataLakeService = new BlobServiceClient(destinationUri);

            var filesCount = 0;
            var processedCount = 0;
            var failedCount = 0;
            var totalcount = 0;
            using var cancellationTokenSource = new CancellationTokenSource();


            using var semaphore = new SemaphoreSlim(threads, threads);

            var sourceFileTasks = new ConcurrentDictionary<Guid, Task>();
            var copyTasks = new ConcurrentDictionary<Guid, Task>();
            var currentContinuationToken = "";
            var currentFileSystem = "";

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
            await foreach (var filesystem in sourceDatalakeService.GetFileSystemsAsync(cancellationToken: cancellationTokenSource.Token))
            {
                currentFileSystem = filesystem.Name;

                var sourceFileSystemClient = sourceDatalakeService.GetFileSystemClient(filesystem.Name);
                var destinationFileSystemClient = destinationDataLakeService.GetBlobContainerClient(filesystem.Name);
                await destinationFileSystemClient.CreateIfNotExistsAsync();

                var sourcePathNames = new BlockingCollection<string>();

                Console.WriteLine("Starting list files task");
                var listFilesTask = Task.Run(async () =>
                {
                    try
                    {
                        await foreach (var sourcePathPage in sourceFileSystemClient.GetPathsAsync(recursive: recursive, cancellationToken: cancellationTokenSource.Token).AsPages(pageSizeHint: 5000))
                        {
                            foreach (var sourcepath in sourcePathPage.Values)
                            {
                                sourcePathNames.Add(sourcepath.Name);
                                var currentCount = Interlocked.Increment(ref filesCount);
                                if (currentCount % 10000 == 0)
                                {
                                    Console.WriteLine($"Found {currentCount} files...");
                                }
                            }

                            currentContinuationToken = sourcePathPage.ContinuationToken;
                        }
                    }
                    catch (TaskCanceledException)
                    {
                        Console.WriteLine("\n\nStopping list path task. To continue from here use token:");
                        Console.WriteLine($"ContinuationToken: {currentContinuationToken}");
                        Console.WriteLine($"FileSystem: {currentFileSystem}\n\n");
                    }
                    finally
                    {
                        sourcePathNames.CompleteAdding();
                        Console.WriteLine($"List files done. Found {filesCount} total files to copy");
                    }
                });

                Console.WriteLine("Starting consume tasks");
                var iterateTask = Task.Run(async () =>
                {
                    while (sourcePathNames.TryTake(out var sourcePath, -1))
                    {
                        await semaphore.WaitAsync();

                        var taskId = Guid.NewGuid();
                        sourceFileTasks.TryAdd(taskId, Task.Run(async () =>
                        {
                            try
                            {
                                var sourceFileClient = sourceFileSystemClient.GetFileClient(sourcePath);
                                var destinationFileClient = destinationFileSystemClient.GetBlobClient(sourcePath);
                                var status = await destinationFileClient.StartCopyFromUriAsync(sourceFileClient.Uri);

                                if (waitForCopyResult)
                                {
                                    copyTasks.TryAdd(taskId, status.WaitForCompletionAsync().AsTask().ContinueWith((o) => { copyTasks.TryRemove(taskId, out _); }));
                                }

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
                                sourceFileTasks.TryRemove(taskId, out _);
                            }
                        }));
                    }

                    Console.WriteLine("Consume task done");
                });

                Console.WriteLine("Waiting for list files and iterate tasks");
                await Task.WhenAll(listFilesTask, iterateTask);
                Console.WriteLine($"Waiting for {sourceFileTasks.Count} source file tasks to complete");
                await Task.WhenAll(sourceFileTasks.Values);
                Console.WriteLine($"All copy tasks have been started, waiting for {copyTasks.Count} to complete...");
                await Task.WhenAll(copyTasks.Values);
            }

            Console.WriteLine($"Done, copy took {stopwatch.Elapsed}");
            Console.WriteLine($"Processed: {processedCount}");
            Console.WriteLine($"Failed: {failedCount}");
            Console.WriteLine($"Total: {totalcount}");
        }
    }
}