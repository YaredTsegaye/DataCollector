using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MTConnectAgent
{
    class StorageClient
    {
        private const string headerFormat = "datetime,unixdatetime,value,sequence";

        /// <summary>
        /// Uploads/ appends data to block blob.
        /// </summary>
        /// <param name="blobUri">Blob Uri</param>
        /// <param name="data">Data to upload</param>
        /// <returns></returns>
        public static async Task UploadFileToBlobAsync(string blobUri, string data)
        {
            var blob = new CloudBlockBlob(new Uri(blobUri));
            // Generate a blob id

            string id = String.Empty;
            int blockCount = 0;

            byte[] byteArray = Encoding.UTF8.GetBytes(data);
            MemoryStream stream = new MemoryStream(byteArray);

            List<string> blockIdList = new List<string>();
            // If blob exists, get the list of ids of uploaded blocks, else create the blob with the header row.
            if (blob.Exists())
            {
                var blockList = blob.DownloadBlockList();
                blockCount = blockList.Count();

                foreach (ListBlockItem item in blockList)
                {
                    blockIdList.Add(item.Name);
                }
            }
            else
            {
                var headerId = Convert.ToBase64String(ASCIIEncoding.ASCII.GetBytes(string.Format("BlockId{0}", (++blockCount).ToString("0000000"))));
                blob.PutBlock(headerId, new MemoryStream(Encoding.UTF8.GetBytes(headerFormat + "\r\n")), null);
                blockIdList.Add(headerId);
            }

            id = Convert.ToBase64String(ASCIIEncoding.ASCII.GetBytes(string.Format("BlockId{0}", (++blockCount).ToString("0000000"))));
            blob.PutBlock(id, stream, null);
            blockIdList.Add(id);
            blob.PutBlockList(blockIdList);
        }

        public static string GenerateBlobUri(string storageConnectionString, string sessionId)
        {
            // Create/ get the blob container reference.
            var storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            var blobClient = storageAccount.CreateCloudBlobClient();
            var blobContainer = blobClient.GetContainerReference("data");
            var list = blobContainer.ListBlobs();
            blobContainer.CreateIfNotExists();

            string[] blobtypes = new string[] { "CONTROLLERMODE", "EXECUTION", "PATHFEEDRATEACTUAL", "PROGRAM", "RAPIDOVERRIDE", "SPINDLESPEEDACTUAL", "TOOLID" };
            StringBuilder paths = new StringBuilder();
            for (int i = 0; i < 7; i++)
            {
                CloudBlockBlob blob = blobContainer.GetBlockBlobReference(string.Format("{0}_{1}", sessionId, blobtypes[i]));

                SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy();
                sasConstraints.SharedAccessStartTime = DateTime.UtcNow.AddMinutes(-5);
                sasConstraints.SharedAccessExpiryTime = DateTime.UtcNow.AddHours(512);
                sasConstraints.Permissions = SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Write;
                string sasBlobToken = blob.GetSharedAccessSignature(sasConstraints);

                paths.Append(blobtypes[i]);
                paths.Append("|");
                paths.Append(blob.Uri + sasBlobToken);
                if (i != 6)
                    paths.Append("|");
            }
            Console.WriteLine("Generated blob paths without IOT hub...");
            Console.WriteLine();
            return paths.ToString();
        }
    }
}
