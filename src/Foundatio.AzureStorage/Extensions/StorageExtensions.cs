using System;
using Foundatio.Storage;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs;

namespace Foundatio.Azure.Extensions {
    public static class StorageExtensions {
        public static FileSpec ToFileInfo(this BlobProperties blob, string name) {
            if (blob.ContentLength == -1)
                return null;

            return new FileSpec {
                Path = name,
                Size = blob.ContentLength,
                Created = blob.CreatedOn.UtcDateTime,
                Modified = blob.LastModified.UtcDateTime
            };
        }


    }
}
