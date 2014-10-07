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
		/// <summary>
		/// We store the exact fieldnames here for the iis log lines so we 
		/// can match those against the values and json-serialize those.
		/// </summary>
		private static readonly string[] IisLogFields =
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
			var values = iisLogLine.Split(' ').ToArray();
			var keyValuePairs = IisLogFields
				.Skip(2) //skip the date and time
				.Select((value, index) => new { index, field = value })
				.Zip(
					iisLogLine.Split(' ')
						.Skip(2)
						.Select(val => val.Replace(@"""", "'")), // escape some json-invalid stuff
					(keyTuple, value) =>
					{
						if (keyTuple.index > 10 || keyTuple.index == 4)//zero based index 4 = port, all above 10 = bytes/response times etc
							return String.Format(CultureInfo.InvariantCulture, @"""{0}"":{1}", keyTuple.field, value);//int values
						return String.Format(CultureInfo.InvariantCulture, @"""{0}"":""{1}""", keyTuple.field, value);//string values
					});

			var timestamp = DateTime.ParseExact(values[0] + " " + values[1], "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal);
			//@timestamp is the ISO8601 high-precision timestamp for the event.
			//@version is the version number of this json schema
			// all other fields are free

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

			return GetNewOrModifiedLogFiles(logsBlobContainer, iisApplicationPrefix, skipUntilModifiedDate)
				#if DEBUG
				.Do(logFile => Console.WriteLine("new or updated file: " + logFile.Uri))
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

		/// <summary>
		/// return an observable which emits strings from the given streamreader
		/// </summary>
		/// <param name="streamReader"></param>
		/// <returns></returns>
		/// <remarks>
		/// Does not dispose the given streamReader
		/// </remarks>
		private static IObservable<string> readLinesFromStream(StreamReader streamReader)
		{
			return Observable.Create<string>(observer =>
			{
				while (!streamReader.EndOfStream)
					observer.OnNext(streamReader.ReadLine());
				return streamReader;
			});
		}

		/// <summary>
		/// Starts monitoring the given blobContainer for new or updated CloubBlockBlobs (logs)
		/// within the given <paramref name="applicationPrefix"/>, optionally starts with 
		/// processing at the given <paramref name="skipUntilModifiedDate"/>
		/// </summary>
		/// <param name="logsBlobContainer">The blobcontainer in which the logfiles will reside</param>
		/// <param name="applicationPrefix">Within the blobcontainer the iisapp name will prefix the logfiles or the application logs</param>
		/// <param name="skipUntilModifiedDate">Logfiles with a modified date before the given date are ignored</param>
		/// <returns>a stream of (logFile) Blobs which are created or updated</returns>
		/// <remarks>
		/// When subscribing to this observable it will keep state of which
		/// blobs it has already emitted. It is the responsibility of the 
		/// subscriber to persist this information so that upon a later 
		/// subscription he has the information to pass a skipUntilModifiedDate
		/// </remarks>
		public static IObservable<CloudBlockBlob> GetNewOrModifiedLogFiles(CloudBlobContainer logsBlobContainer, string applicationPrefix, DateTimeOffset? skipUntilModifiedDate = null)
		{
			return Observable.Create<CloudBlockBlob>(observer =>
			{
				//keep state of the modified dates we have seen
				var lastProcessedModifiedDate = skipUntilModifiedDate ?? DateTimeOffset.MinValue;

				// IIS logs are only published once every 30 seconds on Azure
				var timerObservable = Observable.Interval(TimeSpan.FromSeconds(30))
					.StartWith(-1L)//immediatly fire first event
					.Subscribe(_ =>
					getCloudContainerPrefixes(applicationPrefix, lastProcessedModifiedDate)
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
		/// <param name="applicationPrefix">Can be the IIS app or application_logs prefix</param>
		/// <param name="logsSinceModifiedDate"></param>
		/// <returns></returns>
		private static IEnumerable<string> getCloudContainerPrefixes(string applicationPrefix, DateTimeOffset logsSinceModifiedDate)
		{
			if (logsSinceModifiedDate > DateTimeOffset.UtcNow.roundPrecision(TimeSpan.TicksPerHour))
			{
				//we only need the last hour
				return new[]
					{
					  String.Format(CultureInfo.InvariantCulture, "{0}/{1}",
						applicationPrefix, logsSinceModifiedDate.ToString("yyyy/MM/dd/HH", CultureInfo.InvariantCulture)
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
						applicationPrefix, date.ToString("yyyy/MM/dd", CultureInfo.InvariantCulture)
						);
					});
			}

			//we need them all (or it is at least more efficient to page in memory
			return new[] { applicationPrefix };
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
