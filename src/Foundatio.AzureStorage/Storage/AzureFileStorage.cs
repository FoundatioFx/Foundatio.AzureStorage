using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.IO.Pipelines;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Azure.Extensions;
using Foundatio.Extensions;
using Foundatio.Serializer;
using Foundatio.Utility;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;

namespace Foundatio.Storage {
    public class AzureFileStorage : IFileStorage {
        private const int cacheSize = 64;
        private static readonly Queue<Pipe> s_pipeCache = new Queue<Pipe>(cacheSize);

        private readonly Task _containerCreation;
        private readonly CloudBlobContainer _container;
        private readonly ISerializer _serializer;

        public AzureFileStorage(AzureFileStorageOptions options) {
            if (options == null)
                throw new ArgumentNullException(nameof(options));

            var account = CloudStorageAccount.Parse(options.ConnectionString);
            var client = account.CreateCloudBlobClient();
            _container = client.GetContainerReference(options.ContainerName);
            _containerCreation = _container.CreateIfNotExistsAsync();
            _serializer = options.Serializer ?? DefaultSerializer.Instance;
        }

        public AzureFileStorage(Builder<AzureFileStorageOptionsBuilder, AzureFileStorageOptions> config)
            : this(config(new AzureFileStorageOptionsBuilder()).Build()) { }

        ISerializer IHaveSerializer.Serializer => _serializer;

        public async Task<Stream> GetFileStreamAsync(string path, CancellationToken cancellationToken = default) {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));

            await EnsureContainerCreated().AnyContext();

            var blockBlob = _container.GetBlockBlobReference(path);
            try {
                await blockBlob.FetchAttributesAsync().AnyContext();
                var option = new BlobRequestOptions() {
                    // As we are transmitting over TLS we don't need the MD5 validation
                    DisableContentMD5Validation = false
                };
                var blobStream = await blockBlob.OpenReadAsync(null, option, null, cancellationToken).AnyContext();
                if (blockBlob.Metadata.TryGetValue(StorageExtensions.UncompressedLength, out _)) {
                    // If compressed return decompressing Stream
                    return new GZipStream(blobStream, CompressionMode.Decompress);
                }
                // Otherwise return original Stream
                return blobStream;
            } catch (StorageException ex) {
                if (ex.RequestInformation.HttpStatusCode == 404)
                    return null;

                throw;
            }
        }

        public async Task<FileSpec> GetFileInfoAsync(string path) {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));

            await EnsureContainerCreated().AnyContext();

            var blob = _container.GetBlockBlobReference(path);
            try {
                await blob.FetchAttributesAsync().AnyContext();
                return blob.ToFileInfo();
            } catch (Exception) { }

            return null;
        }

        public async Task<bool> ExistsAsync(string path) {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));

            await EnsureContainerCreated().AnyContext();

            var blockBlob = _container.GetBlockBlobReference(path);
            return await blockBlob.ExistsAsync().AnyContext();
        }

        public async Task<bool> SaveFileAsync(string path, Stream stream, CancellationToken cancellationToken = default) {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));

            if (stream == null)
                throw new ArgumentNullException(nameof(stream));

            await EnsureContainerCreated().AnyContext();

            var blockBlob = _container.GetBlockBlobReference(path);

            // GZipStream doesn't allow it to be the source of compressed data, and needs to go via an intermediate like MemoryStream.
            // Rather than buffering everything in memory this way, we use Pipelines to invert the Stream, so the compression can be
            // streamed on the fly.
            var pipe = RentPipe();
            var gzipStream = new GZipStream(pipe.Writer.AsStream(), CompressionMode.Compress);
            var uploadTask = Task.CompletedTask;
            try {
                if (!stream.CanSeek) {
                    // Need to record the uncompressed size in the metadata
                    stream = new CountingStream(stream);
                }

                var copyTask = stream.CopyToAsync(gzipStream);

                var option = new BlobRequestOptions() {
                    // As we are transmitting over TLS we don't need the MD5 validation
                    DisableContentMD5Validation = false
                };
                uploadTask = blockBlob.UploadFromStreamAsync(pipe.Reader.AsStream(), null, option, null, cancellationToken);

                await copyTask.AnyContext();
                await gzipStream.FlushAsync().AnyContext();
            } finally {
                gzipStream.Dispose();
                await pipe.Writer.CompleteAsync().AnyContext();
                await uploadTask.AnyContext();
                await pipe.Reader.CompleteAsync().AnyContext();
            }

            ReturnPipe(pipe);

            // Set headers
            if (path.EndsWith(".json")) {
                blockBlob.Properties.ContentType = "application/json";
            }
            blockBlob.Properties.ContentEncoding = "gzip";
            await blockBlob.SetPropertiesAsync().AnyContext();
            blockBlob.Metadata.Add(StorageExtensions.UncompressedLength, stream.Length.ToString((IFormatProvider)null));
            await blockBlob.SetMetadataAsync().AnyContext();

            return true;
        }

        public async Task<bool> RenameFileAsync(string path, string newPath, CancellationToken cancellationToken = default) {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));
            if (String.IsNullOrEmpty(newPath))
                throw new ArgumentNullException(nameof(newPath));

            await EnsureContainerCreated().AnyContext();

            var oldBlob = _container.GetBlockBlobReference(path);
            if (!(await CopyFileAsync(path, newPath, cancellationToken).AnyContext()))
                return false;

            return await oldBlob.DeleteIfExistsAsync(DeleteSnapshotsOption.None, null, null, null, cancellationToken).AnyContext();
        }

        public async Task<bool> CopyFileAsync(string path, string targetPath, CancellationToken cancellationToken = default) {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));
            if (String.IsNullOrEmpty(targetPath))
                throw new ArgumentNullException(nameof(targetPath));

            await EnsureContainerCreated().AnyContext();

            var oldBlob = _container.GetBlockBlobReference(path);
            var newBlob = _container.GetBlockBlobReference(targetPath);

            await newBlob.StartCopyAsync(oldBlob, cancellationToken).AnyContext();
            while (newBlob.CopyState.Status == CopyStatus.Pending)
                await SystemClock.SleepAsync(50, cancellationToken).AnyContext();

            return newBlob.CopyState.Status == CopyStatus.Success;
        }

        public async Task<bool> DeleteFileAsync(string path, CancellationToken cancellationToken = default) {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));

            await EnsureContainerCreated().AnyContext();

            var blockBlob = _container.GetBlockBlobReference(path);
            return await blockBlob.DeleteIfExistsAsync(DeleteSnapshotsOption.None, null, null, null, cancellationToken).AnyContext();
        }

        public async Task<int> DeleteFilesAsync(string searchPattern = null, CancellationToken cancellationToken = default) {
            var files = await GetFileListAsync(searchPattern, cancellationToken: cancellationToken).AnyContext();
            int count = 0;

            // TODO: We could batch this, but we should ensure the batch isn't thousands of files.
            foreach (var file in files) {
                await DeleteFileAsync(file.Path, cancellationToken).AnyContext();
                count++;
            }

            return count;
        }

        public async Task<PagedFileListResult> GetPagedFileListAsync(int pageSize = 100, string searchPattern = null, CancellationToken cancellationToken = default) {
            if (pageSize <= 0)
                return PagedFileListResult.Empty;

            var result = new PagedFileListResult(r => GetFiles(searchPattern, 1, pageSize, cancellationToken));
            await result.NextPageAsync().AnyContext();
            return result;
        }

        private async Task<NextPageResult> GetFiles(string searchPattern, int page, int pageSize, CancellationToken cancellationToken) {
            int pagingLimit = pageSize;
            int skip = (page - 1) * pagingLimit;
            if (pagingLimit < Int32.MaxValue)
                pagingLimit++;

            var list = (await GetFileListAsync(searchPattern, pagingLimit, skip, cancellationToken).AnyContext()).ToList();
            bool hasMore = false;
            if (list.Count == pagingLimit) {
                hasMore = true;
                list.RemoveAt(pagingLimit - 1);
            }

            return new NextPageResult {
                Success = true,
                HasMore = hasMore,
                Files = list,
                NextPageFunc = hasMore ? r => GetFiles(searchPattern, page + 1, pageSize, cancellationToken) : (Func<PagedFileListResult, Task<NextPageResult>>)null
            };
        }

        public async Task<IEnumerable<FileSpec>> GetFileListAsync(string searchPattern = null, int? limit = null, int? skip = null, CancellationToken cancellationToken = default) {
            if (limit.HasValue && limit.Value <= 0)
                return new List<FileSpec>();

            searchPattern = searchPattern?.Replace('\\', '/');
            string prefix = searchPattern;
            Regex patternRegex = null;
            int wildcardPos = searchPattern?.IndexOf('*') ?? -1;
            if (searchPattern != null && wildcardPos >= 0) {
                patternRegex = new Regex("^" + Regex.Escape(searchPattern).Replace("\\*", ".*?") + "$");
                int slashPos = searchPattern.LastIndexOf('/');
                prefix = slashPos >= 0 ? searchPattern.Substring(0, slashPos) : String.Empty;
            }
            prefix ??= String.Empty;

            await EnsureContainerCreated().AnyContext();

            BlobContinuationToken continuationToken = null;
            var blobs = new List<CloudBlockBlob>();
            do {
                var listingResult = await _container.ListBlobsSegmentedAsync(prefix, true, BlobListingDetails.Metadata, limit, continuationToken, null, null, cancellationToken).AnyContext();
                continuationToken = listingResult.ContinuationToken;

                // TODO: Implement paging
                blobs.AddRange(listingResult.Results.OfType<CloudBlockBlob>().MatchesPattern(patternRegex));
            } while (continuationToken != null && blobs.Count < limit.GetValueOrDefault(Int32.MaxValue));

            if (limit.HasValue)
                blobs = blobs.Take(limit.Value).ToList();

            return blobs.Select(blob => blob.ToFileInfo());
        }

        private Task EnsureContainerCreated() => _containerCreation;

        private static Pipe RentPipe() {
            var cache = s_pipeCache;
            lock (cache) {
                if (cache.Count > 0)
                    return cache.Dequeue();
            }

            return new Pipe();
        }

        private static void ReturnPipe(Pipe pipe) {
            pipe.Reset();
            var cache = s_pipeCache;
            lock (cache) {
                if (cache.Count < cacheSize)
                    cache.Enqueue(pipe);
            }
        }

        public void Dispose() {}

        // Used to get the uncompressed length from a Stream that doesn't support querying it upfront
        private class CountingStream : Stream {
            private readonly Stream stream;
            private long readLength = 0;

            public CountingStream(Stream stream) {
                this.stream = stream;
            }

            public override long Length => readLength;

            public override long Position { get => readLength; set => throw new NotSupportedException(); }

            public override int Read(byte[] buffer, int offset, int count) {
                int amount = stream.Read(buffer, offset, count);
                readLength += amount;
                return amount;
            }

            public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) {
                int amount = await stream.ReadAsync(buffer, offset, count, cancellationToken);
                readLength += amount;
                return amount;
            }

            public override int EndRead(IAsyncResult asyncResult) {
                int amount = stream.EndRead(asyncResult);
                readLength += amount;
                return amount;
            }

            public override bool CanRead => stream.CanRead;

            public override bool CanSeek => false;

            public override bool CanWrite => false;

            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

            public override void SetLength(long value) => throw new NotSupportedException();

            public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

            public override void Flush() => throw new NotSupportedException();
        }
    }

    internal static class BlobListExtensions {
        internal static IEnumerable<CloudBlockBlob> MatchesPattern(this IEnumerable<CloudBlockBlob> blobs, Regex patternRegex) {
            return blobs.Where(blob => patternRegex == null || patternRegex.IsMatch(blob.ToFileInfo().Path));
        }
    }
}
