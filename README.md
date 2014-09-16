# Azure.LogTailer

Library for tailing your IIS &amp; Application logs being written to the Azure blobstore

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

