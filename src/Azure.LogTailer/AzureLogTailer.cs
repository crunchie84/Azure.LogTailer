using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Azure.LogTailer
{
	public static class AzureLogTailer
	{

		public static readonly string[] iisLogFields =
			"date time s-sitename cs-method cs-uri-stem cs-uri-query s-port cs-username c-ip cs(User-Agent) cs(Cookie) cs(Referer) cs-host sc-status sc-substatus sc-win32-status sc-bytes cs-bytes time-taken"
				.Split(' ')
				.Select(f => f.Replace('(', '_').Replace(")", "").Replace('-', '_'))//remove/replace json-incompatible chars
				.ToArray();
		/// <summary>
		/// parse the given iis logline and return it in json_event logstash compatible json
		/// </summary>
		/// <param name="iisLogLine"></param>
		/// <returns></returns>
		public static string ParseIisLogLineToJsonEvent(string iisLogLine)
		{
			//json_event format:
			//{
			//  "message"    => "hello world",
			//  "@version"   => "1",
			//  "@timestamp" => "2014-04-22T23:03:14.111Z",
			//  "type"       => "stdin",
			//  "host"       => "hello.local"
			//}
			//@timestamp is the ISO8601 high-precision timestamp for the event.
			//@version is the version number of this json schema
			// all other fields are free
			
			//iis log line format
			//#Fields: date time s-sitename cs-method cs-uri-stem cs-uri-query s-port cs-username c-ip cs(User-Agent) cs(Cookie) cs(Referer) cs-host sc-status sc-substatus sc-win32-status sc-bytes cs-bytes time-taken
			//index		  0	    1		  2			3		4				5		6		7			8	9				10			11			12		13			14			15				16		17		18
			var values = iisLogLine.Split(' ').ToArray();

      //string values
		  var keyValuePairs = iisLogFields
		    .Skip(2) //skip the date and time
		    .Select((value, index) => new {index, field = value})
		    .Zip(
		      values
		        .Skip(2)
		        .Select(val => val.Replace(@"""", "'")), // escape some json-invalid stuff
		      (keyTuple, value) =>
		      {
		        if (keyTuple.index > 10 || keyTuple.index == 4)//zero based index 4 = port, all above 10 = bytes/response times etc
		          return String.Format(CultureInfo.InvariantCulture, @"""{0}"":{1}", keyTuple.field, value);//int values
		        return String.Format(CultureInfo.InvariantCulture, @"""{0}"":""{1}""", keyTuple.field, value);//string values
		      });

			//2014-09-10 02:54:41
			var timestamp = DateTime.ParseExact(values[0] +" "+ values[1], "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal);

			return string.Format(CultureInfo.InvariantCulture,
				@"{{""@version"":1, ""@timestamp"":""{0}"",""type"":""iis"",{1}}}", 
					timestamp.ToString("yyyy-MM-ddTHH:mm:ssK"),
					String.Join(",", keyValuePairs)
				);
		}

		/// <summary>
		/// Returns an observable stream of loglines to process 
		/// based on the given stream of newOrModifiedIisLogFiles.
		/// </summary>
		/// <param name="logsBlobContainer">The blobcontainer in which the logfiles will reside</param>
		/// <param name="iisApplicationPrefix">Within the blobcontainer the iisapp name will prefix the logfiles</param>
		/// <param name="skipUntilModifiedDate">Logfiles with a modified date before the given date are ignored</param>
		/// <returns></returns>
		/// <remarks>
		/// Keeps state in itself to know which loglines are already
		/// emitted to the observers.
		/// </remarks>
		public static IObservable<string> GetUnprocessedIisLoglines(CloudBlobContainer logsBlobContainer, string iisApplicationPrefix, DateTimeOffset? skipUntilModifiedDate = null)
		{
			var bytesPerUriAlreadyProcessed = new Dictionary<string, long>();

			return GetNewOrModifiedIisLogFiles(logsBlobContainer, iisApplicationPrefix, skipUntilModifiedDate)
	#if DEBUG
				//.Do(logFile => Console.WriteLine("new or updated file: " + logFile.Uri))
				.Do(_ => Console.Write("X"))
	#endif
				.Select(newOrModifiedLogFile => Observable.Using(
					() =>
					{
						// skip/seek the unprocessed parts
						var blobUrl = newOrModifiedLogFile.Uri.ToString();
						var bytesAlreadyProcessed = bytesPerUriAlreadyProcessed.ContainsKey(blobUrl)
							? bytesPerUriAlreadyProcessed[blobUrl]
							: 0L;
						var lengthToDownload = newOrModifiedLogFile.Properties.Length - bytesAlreadyProcessed;

						var memStream = new MemoryStream();
						// TODO does the using also dispose this memorystream when done with the StreamReader?
						newOrModifiedLogFile.DownloadRangeToStream(memStream, bytesAlreadyProcessed, lengthToDownload);
						memStream.Position = 0; //rewind to first position
						return new StreamReader(memStream);
					},
					streamReader => readLinesFromStream(streamReader)
						.Where(line => !line.StartsWith("#"))
						.Select(line => line.Replace("~1", ""))
					))
 				.SelectMany(l => l);
		}

    //public static IObservable<byte> ToObservable(this MemoryStream source, 
    //  int buffersize, 
    //  IScheduler scheduler)
    //  {
    //  var bytes = Observable.Create<byte>(o =>
    //  {
    //    //...
    //  });
    //  return Observable.Using(() => source, _ => bytes);
    //}

		private static IObservable<string> readLinesFromStream(StreamReader stream)
		{
			return Observable.Create<string>(observer =>
			{
				while (!stream.EndOfStream)
					observer.OnNext(stream.ReadLine());

				return stream;
			});
		}

		private static IEnumerable<string> readLines(Stream stream)
		{
			using (var reader = new StreamReader(stream))
			{
				while (!reader.EndOfStream)
					yield return reader.ReadLine();
			}
		}

		/// <summary>
		/// Starts monitoring the given blobContainer for new or updated logfiles
		/// within the given <paramref name="iisApplicationPrefix"/>, optionally starts with 
		/// processing at the given <paramref name="skipUntilModifiedDate"/>
		/// </summary>
		/// <param name="logsBlobContainer">The blobcontainer in which the logfiles will reside</param>
		/// <param name="iisApplicationPrefix">Within the blobcontainer the iisapp name will prefix the logfiles</param>
		/// <param name="skipUntilModifiedDate">Logfiles with a modified date before the given date are ignored</param>
		/// <returns>a stream of (logFile) Blobs which are created or updated</returns>
		/// <remarks>
		/// When subscribing to this observable it will keep state of which
		/// blobs it has already emitted. It is the responsibility of the 
		/// subscriber to persist this information so that upon a later 
		/// subscription he has the information to pass a skipUntilModifiedDate
		/// </remarks>
		public static IObservable<CloudBlockBlob> GetNewOrModifiedIisLogFiles(CloudBlobContainer logsBlobContainer, string iisApplicationPrefix, DateTimeOffset? skipUntilModifiedDate = null)
		{
			return Observable.Create<CloudBlockBlob>(observer =>
			{
				//keep state of the modified dates we have seen
				var lastProcessedModifiedDate = skipUntilModifiedDate ?? DateTimeOffset.MinValue;

				// IIS logs are only published once every 30 seconds on Azure
				var timerObservable = Observable.Timer(TimeSpan.FromSeconds(30))
				  .StartWith(-1L)//immediatly fire first event
				  .Subscribe(_ =>
					getCloudContainerPrefixes(iisApplicationPrefix, lastProcessedModifiedDate)
					  .Select(prefix => logsBlobContainer.ListBlobs(prefix, true)
						.OfType<CloudBlockBlob>()
						.Where(
						  blob => blob.Properties.LastModified != null && blob.Properties.LastModified > lastProcessedModifiedDate)
					  )
					  .SelectMany(blobs => blobs)
					  .OrderBy(blob => blob.Properties.LastModified)
					  .ToObservable()
					  .Do(blob => lastProcessedModifiedDate = blob.Properties.LastModified ?? lastProcessedModifiedDate)
					  .Subscribe(observer.OnNext, observer.OnError)
					);

				return timerObservable;
			});
		}

		/// <summary>
		/// retrieve a list of blob uri prefixes to retrieve to minimize azure transactions
		/// </summary>
		/// <param name="iisApplicationPrefix"></param>
		/// <param name="logsSinceModifiedDate"></param>
		/// <returns></returns>
		private static IEnumerable<string> getCloudContainerPrefixes(string iisApplicationPrefix, DateTimeOffset logsSinceModifiedDate)
		{
			if (logsSinceModifiedDate > DateTimeOffset.UtcNow.roundPrecision(TimeSpan.TicksPerHour))
			{
				//we only need the last hour
				return new[]
				{
					String.Format(CultureInfo.InvariantCulture, "{0}/{1}",
						iisApplicationPrefix, logsSinceModifiedDate.ToString("yyyy/MM/dd/HH", CultureInfo.InvariantCulture)
					)
				};
			}

			if (logsSinceModifiedDate > DateTimeOffset.UtcNow.Date.AddDays(-7))
			{
				//we need the last 1 .. 7 days
				return Enumerable.Range(0, 1 + (DateTimeOffset.UtcNow - logsSinceModifiedDate).Days)
				  .Select(offset =>
				  {
					  var date = DateTimeOffset.UtcNow.Date.AddDays(offset * -1);
					  return String.Format(CultureInfo.InvariantCulture, "{0}/{1}",
						iisApplicationPrefix, date.ToString("yyyy/MM/dd", CultureInfo.InvariantCulture)
					  );
				  });
			}

			//we need them all (or it is at least more efficient to page in memory
			return new[] { iisApplicationPrefix };
		}

		/// <summary>
		/// round the precision of the given datetimeoffset with the amount of <paramref name="roundTicks"/>
		/// </summary>
		/// <param name="date"></param>
		/// <param name="roundTicks"></param>
		/// <returns></returns>
		private static DateTimeOffset roundPrecision(this DateTimeOffset date, long roundTicks)
		{
			return date.Subtract(TimeSpan.FromTicks(date.Ticks % roundTicks));
		}
	}
}
