using System;
using Foundatio.Storage;
using Microsoft.Azure.Storage.Blob;

namespace Foundatio.Azure.Extensions {
    public static class StorageExtensions {
        internal const string UncompressedLength = "XUncompressedLength";
        public static FileSpec ToFileInfo(this CloudBlockBlob blob) {
            if (!blob.Metadata.TryGetValue(UncompressedLength, out string lengthStr) || 
                !Int64.TryParse(lengthStr, out long length))
            {
                length = blob.Properties.Length;
            }

            if (length == -1)
                return null;

            return new FileSpec {
                Path = blob.Name,
                Size = length,
                Created = blob.Properties.LastModified?.UtcDateTime ?? DateTime.MinValue,
                Modified = blob.Properties.LastModified?.UtcDateTime ?? DateTime.MinValue
            };
        }
    }
}
