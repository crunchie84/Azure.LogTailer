# Azure.LogTailer

Library for tailing your IIS &amp; Application logs being written to the Azure blobstore

# Usage

Due to the close integration of Azure WebJobs with Azure Websites this repository will result in a Console application which can run as a continuous running Azure WebJob. It does not actually use much of the webjob SDK but the wrapping of the console application.

# How does it work

The Console application will start searching for (IIS) logfiles within the configured blobcontainer. All logfiles which are not processed yet are emitted as json messages to the configured redis cache so you can further process those with logstash. When the Console application has emitted the loglines it will store those in a blob next to the IIS logs so it can auto-resume when it has been stopped.

# Compiling it yourself

- Visual Studio 2013
- NuGet Package Restore FTW

The repository also contains an .editorconfig file which helps to maintain consistent tabs vs spaces & line endings. You can install the [Visual Studio plugin](http://visualstudiogallery.msdn.microsoft.com/c8bccfe2-650c-4b42-bc5c-845e21f96328) which will detect this file and automatically update your editor settings.


# Costs

This library will poll the blobstore for changes thus use transactions. I have counted the following behaviour:

## Finding new or modified files

* Initial listing of blobs -> 1 call 
* last modified > 1 hour < 7 days -> 1 call per day segment 
* last modified < 1 hour -> 1 call

## Retrieving the files

* 1 call per filepart modified

This will total if your application writes 1 logfile per hour in chunks of 30 seconds to the following amount of calls per day:

* 1 (modified last hour) * 2 (per 30 seconds) * 60 (hour) * 24 (day) => 2880 calls to the blobstore for listing files
* 1 (file modified found per 30 seconds) * 2 (per 30 seconds) * 60 (hour) * 24 (day) => 2880 calls to the blobstore for retrieving (partial) files

Total transactions per day (naive) 5760 per day. Find the pricing for your region on the [Azure website](http://azure.microsoft.com/en-us/pricing/details/storage/)

