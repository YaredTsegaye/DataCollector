using Microsoft.Azure.Devices;
using Microsoft.Azure.Devices.Client;
using MTConnectSharp;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;


namespace MTConnectAgent
{
    /// <summary>
    /// This class get the data blob URLs from IoT hub, captures the data from MTConnect agent and uploads it to blobs.
    /// </summary>
    class DataCollector
    {
        private string machineName;
        private string iotHubConnectionString;
        private string storageConnectionString;
        private string baseURL;
        private string sessionId;
        private int sampleInterval;
        private int recordCount;

        private const string dataFormat = "{0},{1},{2},{3}";
        private DeviceClient deviceClient;
        private MTConnectClient client;

        private Dictionary<string, string> blobpaths = new Dictionary<string, string>();
        private StringBuilder sbControllerMode = new StringBuilder();
        private StringBuilder sbExecution = new StringBuilder();
        private StringBuilder sbPathFeedrateActual = new StringBuilder();
        private StringBuilder sbProgram = new StringBuilder();
        private StringBuilder sbRapidOverride = new StringBuilder();
        private StringBuilder sbSpindleSpeed = new StringBuilder();
        private StringBuilder sbToolId = new StringBuilder();

        private string[] blobtypes = new string[] { "CONTROLLERMODE", "EXECUTION", "PATHFEEDRATEACTUAL", "PROGRAM", "RAPIDOVERRIDE", "SPINDLESPEEDACTUAL", "TOOLID" };

        public DataCollector(string MachineName, string IoTHubConnectionString, string StorageConnectionString, string BaseURL, int SampleInterval, int RecordCount)
        {
            this.machineName = MachineName;
            this.iotHubConnectionString = IoTHubConnectionString;
            this.storageConnectionString = StorageConnectionString;
            this.baseURL = BaseURL;
            this.sampleInterval = SampleInterval;
            this.recordCount = RecordCount;

            // Unique session id which will be the prefix for blob names.
            sessionId = machineName + "/" + DateTime.Now.ToString().Replace(' ', '_').Replace('/', '_').Replace(':', '_');
        }

        /// <summary>
        /// Initializes a device client.
        /// </summary>
        public void Init()
        {
            //TODO: make the registration to be a separate method
            RegistryManager registryManager = RegistryManager.CreateFromConnectionString(iotHubConnectionString);
            var device = registryManager.GetDeviceAsync(machineName).Result;
            if (device == null)
            {
                device = registryManager.AddDeviceAsync(new Microsoft.Azure.Devices.Device(machineName)).Result;
            }

            string deviceConnStr = string.Format("{0};DeviceId={1};SharedAccessKey={2}",
                iotHubConnectionString.Split(new char[] { ';' }).Where(m => m.Contains("HostName")).FirstOrDefault(),
                device.Id, device.Authentication.SymmetricKey.PrimaryKey);

            // Use below 2 lines with IOT hub only
            deviceClient = DeviceClient.CreateFromConnectionString(deviceConnStr, Microsoft.Azure.Devices.Client.TransportType.Http1);
            //await deviceClient.OpenAsync();

            // Use below 2 lines when not using IOT hub only
            //string paths = GenerateBlobUri();
            //GetBlobUris(paths);

            SendD2CMessage();
        }

        #region Private Methods

        /// <summary>
        /// Sends a message indicating the 'deviceready' state to the IoT hub.
        /// </summary>
        /// 
        private void SendD2CMessage()
        {
            var readmsg = new
            {
                DeviceName = machineName,
                SessionId = sessionId,
                BlobNames = JsonConvert.SerializeObject(blobtypes)
            };
            var msgString = JsonConvert.SerializeObject(readmsg);
            var msg = new Microsoft.Azure.Devices.Client.Message(Encoding.ASCII.GetBytes(msgString));
            msg.Properties["messageType"] = "deviceready";
            msg.Properties["sessionId"] = sessionId;
            msg.MessageId = Guid.NewGuid().ToString();

            deviceClient.SendEventAsync(msg);
            Console.WriteLine("Sending message indicating ready state for the machine {0}...", machineName);
            Console.WriteLine();

            ReceiveC2dMessage();
        }

        private  async Task SendDeviceToCloudMessagesAsync(string blobUrl, string data)
        {
            while (true)
            {
                var telemetryDataPoint = new
                {
                    DeviceName = machineName,
                    SessionId = sessionId,
                    BlobUrl = blobUrl,
                    MachineData = data
                };
                
                var messageString = JsonConvert.SerializeObject(telemetryDataPoint);
                var message = new Microsoft.Azure.Devices.Client.Message(Encoding.ASCII.GetBytes(messageString));
                message.Properties["messageType"] = "machineData";
                message.Properties["sessionId"] = sessionId;

                await deviceClient.SendEventAsync(message);
                Console.WriteLine("{0} > Sending message: {1}", DateTime.Now, messageString);

                Task.Delay(1000).Wait();
            }
        }

        /// <summary>
        /// Receives the message from IoT hub with blob URLs.
        /// </summary>
        private void ReceiveC2dMessage()
        {
            while (true)
            {
                Microsoft.Azure.Devices.Client.Message receivedMessage = deviceClient.ReceiveAsync().Result;
                if (receivedMessage == null) continue;

                var msgStr = System.Text.Encoding.Default.GetString(receivedMessage.GetBytes());
                dynamic msgData = JsonConvert.DeserializeObject(msgStr);
                if (msgData.messageType != null && msgData.messageType == "fileupload")
                {
                    GetBlobUris(Convert.ToString(msgData.fileUri));
                    Console.WriteLine("Received the blob paths from the IOT hub for the machine {0}...", machineName);
                    Console.WriteLine();

                    if (blobpaths.Count != blobtypes.Length )
                    {
                        Console.WriteLine("The required number of blob paths for the machine {0}.", machineName);
                        Console.WriteLine();
                        deviceClient.CompleteAsync(receivedMessage);
                        return;
                    }
                    Console.WriteLine();
                    Console.WriteLine("The blob paths for the machine {0} are (container/blob):", machineName);
                    Console.WriteLine();
                    foreach (string str in blobtypes)
                    {
                        Console.WriteLine("{0}/{1}.csv", Convert.ToString(msgData.blobContainerName), str);
                    }
                    Console.WriteLine();
                    Console.WriteLine();
                    Task.Factory.StartNew(() =>    // Begin task
                    {
                        Console.Write("Reading data from the machine {0}", machineName);
                        while (true)
                        {
                            Thread.Sleep(1000);
                            Console.Write(".");
                            Thread.Sleep(1000);
                            Console.Write(".");
                            Thread.Sleep(1000);
                            Console.Write(".");
                            Thread.Sleep(1000);
                            Console.Write("\b \b\b \b\b \b");
                            Thread.Sleep(1000);
                        }
                    });
                    deviceClient.CompleteAsync(receivedMessage);
                    List < string> targettags = (new string[] {"mode","controllermode", "execution", "program"
                        , "toolnumber", "path_feedrate1", "pathfeedrate", "s1speed", "spindlespeed", "rapidoverride" }).ToList();

                    // Initialize an instance of MTConnectClient
                    client = new MTConnectClient(baseURL + machineName, targettags, recordCount);
                    // Register for events
                    client.ProbeCompleted += client_ProbeCompleted;
                    client.DataItemChanged += client_DataItemChanged;
                    client.DataItemsChanged += client_DataItemsChanged;
                    client.UpdateInterval = sampleInterval;
                    client.Probe();
                    
                    break;
                }
            }
        }

        /// <summary>
        /// Retrieves blob URLs form the data string returned by IoT hub.
        /// </summary>
        /// <param name="data">String containing blob URLs.</param>
        private void GetBlobUris(string data)
        {
            string[] blobs = data.Split(new char[] { '|' });
            int i = 0;
            int cnt = blobtypes.Length * 2;
            while (i < cnt)
            {
                blobpaths.Add(blobs[i++], blobs[i++]);
            }
        }

        #endregion

        #region MTConnect Client Event Handlers

        void client_DataItemsChanged(object sender, EventArgs e)
        {
            IList<Task> tasks = new List<Task>();

            // Check if there is any data for upload and upload it if available.
            for (int i = 0; i < blobpaths.Count; i++)
            {
                KeyValuePair<string, string> item = blobpaths.ElementAt(i);
                switch (item.Key)
                {
                    case "CONTROLLERMODE":
                        if (sbControllerMode.Length > 0)
                        {
                            //Console.WriteLine("ControllerMode Length - {0}", sbControllerMode.Length);
                            //tasks.Add(StorageClient.UploadFileToBlobAsync(item.Value, sbControllerMode.ToString()));
                            tasks.Add(SendDeviceToCloudMessagesAsync(item.Value, sbControllerMode.ToString()));
                            sbControllerMode.Clear();
                        }
                        break;
                    case "EXECUTION":
                        if (sbExecution.Length > 0)
                        {
                            //Console.WriteLine("Execution Length - {0}", sbExecution.Length);
                            //tasks.Add(StorageClient.UploadFileToBlobAsync(item.Value, sbExecution.ToString()));
                            tasks.Add(SendDeviceToCloudMessagesAsync(item.Value, sbExecution.ToString()));
                            sbExecution.Clear();
                        }
                        break;
                    case "PROGRAM":
                        if (sbProgram.Length > 0)
                        {
                            //Console.WriteLine("Program Length - {0}", sbProgram.Length);
                            //tasks.Add(StorageClient.UploadFileToBlobAsync(item.Value, sbProgram.ToString()));
                            tasks.Add(SendDeviceToCloudMessagesAsync(item.Value, sbProgram.ToString()));
                            sbProgram.Clear();
                        }
                        break;
                    case "TOOLID":
                        if (sbToolId.Length > 0)
                        {
                            //Console.WriteLine("ToolId Length - {0}", sbToolId.Length);
                            //tasks.Add(StorageClient.UploadFileToBlobAsync(item.Value, sbToolId.ToString()));
                            tasks.Add(SendDeviceToCloudMessagesAsync(item.Value, sbToolId.ToString()));
                            sbToolId.Clear();
                        }
                        break;
                    case "PATHFEEDRATEACTUAL":
                        if (sbPathFeedrateActual.Length > 0)
                        {
                            //Console.WriteLine("PathFeedrateActual Length - {0}", sbPathFeedrateActual.Length);
                            //tasks.Add(StorageClient.UploadFileToBlobAsync(item.Value, sbPathFeedrateActual.ToString()));
                            tasks.Add(SendDeviceToCloudMessagesAsync(item.Value, sbPathFeedrateActual.ToString()));
                            sbPathFeedrateActual.Clear();
                        }
                        break;
                    case "SPINDLESPEEDACTUAL":
                        if (sbSpindleSpeed.Length > 0)
                        {
                            //Console.WriteLine("SpindleSpeed Length - {0}", sbSpindleSpeed.Length);
                            //tasks.Add(StorageClient.UploadFileToBlobAsync(item.Value, sbSpindleSpeed.ToString()));
                            tasks.Add(SendDeviceToCloudMessagesAsync(item.Value, sbSpindleSpeed.ToString()));
                            sbSpindleSpeed.Clear();
                        }
                        break;
                    case "RAPIDOVERRIDE":
                        if (sbRapidOverride.Length > 0)
                        {
                            //Console.WriteLine("RapidOverride Length - {0}", sbRapidOverride.Length);
                            //tasks.Add(StorageClient.UploadFileToBlobAsync(item.Value, sbRapidOverride.ToString()));
                            tasks.Add(SendDeviceToCloudMessagesAsync(item.Value, sbRapidOverride.ToString()));
                            sbRapidOverride.Clear();
                        }
                        break;
                }
            }

            Task.WaitAll(tasks.ToArray());
            //Console.WriteLine("Upload Event Completed - {0} \n", DateTime.Now.ToShortTimeString());
        }

        void client_DataItemChanged(object sender, DataItemChangedEventArgs e)
        {
            
            // Calculate UNIX time for the data item timestamp
            DateTime dt = e.DataItem.CurrentSample.TimeStamp;
            DateTime sTime = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            double unixTime = (double)(dt.Subtract(sTime)).TotalSeconds;
            string dtStr = dt.ToString("MM/dd/yyyy hh:mm:ss.fff tt");
            // Capture the data for the required data items
            switch (e.DataItem.Name.ToString().ToLower())
            {
                case "mode":
                case "controllermode":
                    sbControllerMode.AppendLine(string.Format(dataFormat, dtStr, unixTime, e.DataItem.CurrentSample.Value, e.DataItem.CurrentSample.Sequence));
                    break;
                case "execution":
                    sbExecution.AppendLine(string.Format(dataFormat, dtStr, unixTime, e.DataItem.CurrentSample.Value, e.DataItem.CurrentSample.Sequence));
                    break;
                case "program":
                    sbProgram.AppendLine(string.Format(dataFormat, dtStr, unixTime, e.DataItem.CurrentSample.Value, e.DataItem.CurrentSample.Sequence));
                    break;
                case "toolnumber":
                    sbToolId.AppendLine(string.Format(dataFormat, dtStr, unixTime, e.DataItem.CurrentSample.Value, e.DataItem.CurrentSample.Sequence));
                    break;
                case "path_feedrate1":
                case "pathfeedrate":
                    if (e.DataItem.SubType.ToUpper() == "ACTUAL")
                    {
                        sbPathFeedrateActual.AppendLine(string.Format(dataFormat, dtStr, unixTime, e.DataItem.CurrentSample.Value, e.DataItem.CurrentSample.Sequence));
                    }
                    break;
                case "s1speed":
                case "spindlespeed":
                    if (e.DataItem.SubType.ToUpper() == "ACTUAL")
                    {
                        sbSpindleSpeed.AppendLine(string.Format(dataFormat, dtStr, unixTime, e.DataItem.CurrentSample.Value, e.DataItem.CurrentSample.Sequence));
                    }
                    break;
                case "rapidoverride":
                    sbRapidOverride.AppendLine(string.Format(dataFormat, dtStr, unixTime, e.DataItem.CurrentSample.Value, e.DataItem.CurrentSample.Sequence));
                    break;
            }
        }

        void client_ProbeCompleted(object sender, EventArgs e)
        {
            var client = sender as MTConnectClient;
            client.StartStreaming();
        }

        #endregion
    }
}
