using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Configuration;
using MTConnectSharp;
using System.Threading;
using System.IO;
using System.Text;

namespace MTConnectAgent
{
    class Program
	{
        static void Main(string[] args)
		{
            DateTime m = DateTime.Now;
            DateTime n = DateTime.UtcNow;
            string errorLogPath = ConfigurationManager.AppSettings["ErrorLogPath"].ToString();

            if (!errorLogPath.EndsWith("/"))
                errorLogPath = errorLogPath + "/";

            string filename = errorLogPath + DateTime.Now.ToString().Replace(' ', '_').Replace('/', '_').Replace(':', '_') + ".txt";
            try
            {
                Init();
            }
            catch(Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
                
                if (!Directory.Exists(errorLogPath))
                    Directory.CreateDirectory(errorLogPath);
                StringBuilder error = new StringBuilder("******************************************************************************");
                error.Append(e.Message);
                error.Append("\r\n\r\n");
                error.Append(e.StackTrace);
                error.Append("******************************************************************************");

                File.WriteAllText(filename, error.ToString());
                Console.WriteLine();
                Console.WriteLine();
                Console.WriteLine("Error details hav been written to the log file: {0}", filename);

                Console.ReadLine();
            }
        }

        static void Init()
        {
            string iotHubConnectionString = ConfigurationManager.AppSettings["IoTHubConnectionString"].ToString();
            string storageConnectionString = ConfigurationManager.AppSettings["StorageConnectionString"].ToString();
            string baseURL = ConfigurationManager.AppSettings["BaseURL"].ToString();
            int sampleInterval = Convert.ToInt32(ConfigurationManager.AppSettings["SampleInterval"]);
            int recordCount = Convert.ToInt32(ConfigurationManager.AppSettings["RecordCount"]);

            Console.WriteLine("Getting the list of devices from the agent...");
            Console.WriteLine();
            List<string> machines = MTConnectClient.GetDeviceList(baseURL);
            List<string> deviceToRead = ConfigurationManager.AppSettings["DeviceToRead"].ToString().Split(new char[] { ',' }).ToList();

            Dictionary<string, Task> collectorTasks = new Dictionary<string, Task>();
            foreach (string dev in deviceToRead)
            {
                if (!machines.Contains(dev))
                {
                    Console.WriteLine("The agent is currently not streaming any data from the device {0}", dev);
                    Console.WriteLine();
                    continue;
                }
                else
                {
                    Task task = Task.Factory.StartNew(() =>    // Begin task
                    {
                        new DataCollector(dev, iotHubConnectionString, storageConnectionString, baseURL, sampleInterval, recordCount).Init();
                    });
                    collectorTasks.Add(dev, task);
                }
            }
            //Console.WriteLine("Press enter to stop streaming.");
            Console.ReadLine();
        }
    }
}
