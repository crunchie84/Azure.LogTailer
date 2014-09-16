using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Reactive.Concurrency;

namespace Azure.LogTailer
{
	public static class AzureLogTailer
	{
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
			return Observable.Create<string>(observer =>
			{
				var bytesPerUriAlreadyProcessed = new Dictionary<string, long>();

				var newOrModifiedLogFilesSubscription = GetNewOrModifiedIisLogFiles(logsBlobContainer, iisApplicationPrefix,
					skipUntilModifiedDate)
					.Subscribe(newOrModifiedLogFile =>
					{
						// skip/seek the unprocessed parts
						var blobUrl = newOrModifiedLogFile.Uri.ToString();
						var bytesAlreadyProcessed = bytesPerUriAlreadyProcessed.ContainsKey(blobUrl)
							? bytesPerUriAlreadyProcessed[blobUrl]
							: 0L;
						var lengthToDownload = newOrModifiedLogFile.Properties.Length - bytesAlreadyProcessed;

						using (var memStream = new MemoryStream())
						{
							newOrModifiedLogFile.DownloadRangeToStream(memStream, bytesAlreadyProcessed, lengthToDownload);
							//memStream.Position = 0;//rewind to first position

							ReadLines(memStream)
								.ToObservable(Scheduler.Default)
								.Where(line => !line.StartsWith("#"))
								.Select(line => line.Replace("~1", ""))
								.Subscribe(observer.OnNext, observer.OnError);
							//TODO does the async observer cause the memorystream to be disposed?
						}
					});

				return newOrModifiedLogFilesSubscription;
			});
		}

		private static IEnumerable<string> ReadLines(Stream stream)
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
