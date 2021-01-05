using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Azure.Extensions;
using Foundatio.Extensions;
using Foundatio.Serializer;
using Foundatio.Utility;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace Foundatio.Storage {
    public class AzureFileStorage : IFileStorage {
        private readonly BlobContainerClient _container;
        private readonly ISerializer _serializer;

        public AzureFileStorage(AzureFileStorageOptions options) {
            if (options == null)
                throw new ArgumentNullException(nameof(options));
            // The storage account used via BlobServiceClient. / Create a BlobServiceClient object which will be used to create a container client
            //BlobServiceClient blobServiceClient = new BlobServiceClient(options.ConnectionString);
            _container = new BlobContainerClient(options.ConnectionString, options.ContainerName);
            _container.CreateIfNotExistsAsync().GetAwaiter().GetResult();
            _serializer = options.Serializer ?? DefaultSerializer.Instance;
        }

        public AzureFileStorage(Builder<AzureFileStorageOptionsBuilder, AzureFileStorageOptions> config)
            : this(config(new AzureFileStorageOptionsBuilder()).Build()) { }

        ISerializer IHaveSerializer.Serializer => _serializer;

        public async Task<Stream> GetFileStreamAsync(string path, CancellationToken cancellationToken = default) {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));

            var blockBlob = _container.GetBlobClient(path);
            try {
                return await blockBlob.OpenReadAsync(null, cancellationToken).AnyContext();
                // All Blob service operations will throw a RequestFailedException instead of StorageException in v11 on failure with helpful ErrorCode
            } catch (RequestFailedException ex) {
                if (ex.ErrorCode == BlobErrorCode.BlobNotFound)
                    return null;

                throw;
            }
        }

        public async Task<FileSpec> GetFileInfoAsync(string path) {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));
            var blob = _container.GetBlobClient(path);
            try {
                BlobProperties properties = await blob.GetPropertiesAsync().AnyContext();
                return properties.ToFileInfo(blob.Name);
            } catch (RequestFailedException) {

            }

            return null;
        }

        public Task<bool> ExistsAsync(string path) {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));

            var blockBlob = _container.GetBlobClient(path);
            var response =  blockBlob.ExistsAsync();
            return Task.FromResult(response.Result.Value);
        }

        public async Task<bool> SaveFileAsync(string path, Stream stream, CancellationToken cancellationToken = default) {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));

            if (stream == null)
                throw new ArgumentNullException(nameof(stream));
            
            var blockBlob = _container.GetBlobClient(path);
            try {
                var contentInfo = await blockBlob.UploadAsync(stream, cancellationToken).AnyContext();
            }
            catch(RequestFailedException) {
               
            }

            return true;
        }

        public async Task<bool> RenameFileAsync(string path, string newPath, CancellationToken cancellationToken = default) {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));
            if (String.IsNullOrEmpty(newPath))
                throw new ArgumentNullException(nameof(newPath));

            var oldBlob = _container.GetBlobClient(path);
            if (!(await CopyFileAsync(path, newPath, cancellationToken).AnyContext()))
                return false;

            return await oldBlob.DeleteIfExistsAsync().AnyContext();
        }

        public async Task<bool> CopyFileAsync(string path, string targetPath, CancellationToken cancellationToken = default) {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));
            if (String.IsNullOrEmpty(targetPath))
                throw new ArgumentNullException(nameof(targetPath));

            var oldBlob = _container.GetBlobClient(path);
            var newBlob = _container.GetBlobClient(targetPath);

            var val = await newBlob.StartCopyFromUriAsync(oldBlob.Uri,null, cancellationToken).AnyContext();
            await val.WaitForCompletionAsync(cancellationToken);
            return val.HasCompleted;
        }


        public Task<bool> DeleteFileAsync(string path, CancellationToken cancellationToken = default) {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));

            var blockBlob = _container.GetBlobClient(path);
            var result = blockBlob.DeleteIfExistsAsync();
            return Task.FromResult(result.Result.Value);
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
                pagingLimit = pagingLimit + 1;

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
            prefix = prefix ?? String.Empty;

            var blobs = new List<BlobClient>();
            var patternMatchingBlobs = new List<BlobClient>();
            await foreach (BlobItem blob in _container.GetBlobsAsync(BlobTraits.Metadata, BlobStates.All, prefix,cancellationToken)) {
                blobs.Add (_container.GetBlobClient(blob.Name));
            }

            if (limit.HasValue)
                blobs = blobs.Take(limit.Value).ToList();

            var filter = blobs.MatchesPattern(patternRegex);
            patternMatchingBlobs.AddRange(filter);


            List<FileSpec> list = new List<FileSpec>();
            foreach(BlobClient patternMatchingBlob in patternMatchingBlobs) {
                BlobProperties properties = await patternMatchingBlob.GetPropertiesAsync().AnyContext();
                list.Add(properties.ToFileInfo(patternMatchingBlob.Name));
            }

            return list.ToArray();
        }

        public void Dispose() {}
    }

    internal static class BlobListExtensions {
        internal static IEnumerable<BlobClient> MatchesPattern(this IEnumerable<BlobClient> blobs, Regex patternRegex) {
            return blobs.Where(blob => patternRegex == null || patternRegex.IsMatch(blob.Name));
        }
    }
}
