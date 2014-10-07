using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;

namespace Azure.LogTailer.Redis.Cli
{
	class Program
	{
		public static void Main()
		{
			JobHost jobHost = new JobHost();

			jobHost.RunAndBlock();
			//jobHost.Call(typeof(Cleanup).GetMethod("CleanupFunction"));

			//connection strings
			//AzureWebJobsStorage
			//AzureWebJobsDashboard
			//These connection strings are used by the WebJobs SDK, one for application data and one for logging. As you saw earlier, the one for application data is also used by the web frontend code.
		}
	}
}
