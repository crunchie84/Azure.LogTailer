using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reactive.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Azure.LogTailer
{
  /// <summary>
  /// The logical logfile tail event 'there are new bytes in this blob available to process'
  /// </summary>
  //public sealed class LogFileBlob
  //{
  //  public CloudBlockBlob BlockBlob { get; set; }
  //  public long ByteOffsetAlreadyProcessed { get; set; }
  //  public long TotalBytesAvailableInBlob { get; set; }
  //}

  public static class AzureLogTailer
  {
    /// <summary>
    /// retrieve a list of blob uri prefixes to retrieve to minimize azure transactions
    /// </summary>
    /// <param name="iisApplicationPrefix"></param>
    /// <param name="logsSinceModifiedDate"></param>
    /// <returns></returns>
    private static IEnumerable<string> getCloudContainerPrefixes(string iisApplicationPrefix, DateTimeOffset? logsSinceModifiedDate = null)
    {
      if (logsSinceModifiedDate.HasValue && logsSinceModifiedDate > DateTimeOffset.UtcNow.roundPrecision(TimeSpan.TicksPerHour))
      {
        //we only need the last hour
        return new[]
        {
          String.Format(CultureInfo.InvariantCulture,
            "{0}/{1}/{2:00}/{3:00}/{4:00}",
            iisApplicationPrefix, logsSinceModifiedDate.Value.Year, logsSinceModifiedDate.Value.Month,
            logsSinceModifiedDate.Value.Day, logsSinceModifiedDate.Value.Hour)
        };
      }

      if (logsSinceModifiedDate.HasValue && logsSinceModifiedDate > DateTimeOffset.UtcNow.Date.AddDays(-7))
      {
        //we need the last 1 .. 7 days
        return Enumerable.Range(0, 1 + (DateTimeOffset.UtcNow - logsSinceModifiedDate).Value.Days)
          .Select(offset =>
          {
            var date = DateTime.Today.AddDays(offset*-1);
            return String.Format(CultureInfo.InvariantCulture,
              "{0}/{1}/{2:00}/{3:00}",
              iisApplicationPrefix, date.Year, date.Month, date.Day);
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
    static DateTimeOffset roundPrecision(this DateTimeOffset date, long roundTicks)
    {
      return date.Subtract(TimeSpan.FromTicks(date.Ticks % roundTicks));
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
        var lastProcessedModifiedDate = skipUntilModifiedDate ?? DateTimeOffset.MinValue;

        // IIS logs are only published once every 30 seconds on Azure
        var timerObservable = Observable.Timer(TimeSpan.FromSeconds(30)).Subscribe(timer =>
        {
          //find new logfiles and publish those to the observer.onNext
          if (lastProcessedModifiedDate <= DateTimeOffset.MinValue)
          {
            //return all files currently available in the blobstore
            // return all files in the container
            logsBlobContainer.ListBlobs(iisApplicationPrefix, true)
              .OfType<CloudBlockBlob>()
              .OrderBy(blob => blob.Properties.LastModified)
              .ToObservable()
              .Do(blob => lastProcessedModifiedDate = blob.Properties.LastModified ?? lastProcessedModifiedDate) //keep state of what we have seen
              .Subscribe(observer);
          }
          else
          {
            // chunk the retrieval of files out of the blob container per day
            //TODO we can improve this code greatly - the blobs are chunked into virtual directories yyyy/mm/dd/hh so we can optimize to only retrieve the last hour if modifieddate last hour
            //TODO extract the listBlobs calls to a separate call so we can remove this if statement all together
            var lastModifiedDay = lastProcessedModifiedDate.Date;
            Enumerable.Range(0, 1 + (DateTime.Today - lastModifiedDay).Days)
            .Select(offset =>
            {
              var date = DateTime.Today.AddDays(offset * -1);
              var blobContainerPrefix = String.Format(CultureInfo.InvariantCulture,
                "{0}/{1}/{2:00}/{3:00}",
                iisApplicationPrefix, date.Year, date.Month, date.Day);

              return logsBlobContainer
                .ListBlobs(blobContainerPrefix, true)
                .OfType<CloudBlockBlob>()
                .Where(blob => blob.Properties.LastModified != null && blob.Properties.LastModified > lastProcessedModifiedDate)
                .OrderBy(blob => blob.Properties.LastModified);
            })
            .SelectMany(date => date)
            .ToObservable()
            .Do(blob => lastProcessedModifiedDate = blob.Properties.LastModified ?? lastProcessedModifiedDate) //keep state of what we have seen
            .Subscribe(observer);//TODO: does this onComplete the observer stream when the enumeration finishes first iteration?
          }
        });

        return timerObservable;//option to dispose the subscription
      });
    }
  }
}
