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
  public sealed class LogFileBlob
  {
    public CloudBlockBlob BlockBlob { get; set; }
    public long ByteOffsetAlreadyProcessed { get; set; }
    public long TotalBytesAvailableInBlob { get; set; }
  }

  public static class AzureLogTailer
  {
    /// <summary>
    /// Starts monitoring the given blobContainer for new logfile blobs or bytes within those 
    /// within the given <paramref name="iisApplicationPrefix"/>, optionally starts with 
    /// processing at the given <paramref name="skipUntilModifiedDate"/>
    /// </summary>
    /// <param name="blobContainer"></param>
    /// <param name="iisApplicationPrefix"></param>
    /// <param name="skipUntilModifiedDate">if given logfiles with a modified date before the given date are ignored</param>
    /// <returns></returns>
    public static IObservable<LogFileBlob> GetUnprocessedIisLogFiles(CloudBlobContainer blobContainer, string iisApplicationPrefix, DateTimeOffset? skipUntilModifiedDate)
    {
      return Observable.Create<LogFileBlob>(observer =>
      {
        var lastProcessedModifiedDate = skipUntilModifiedDate ?? DateTimeOffset.MinValue;

        var timerObservable = Observable.Timer(TimeSpan.FromSeconds(30)).Subscribe(timer =>
        {
          //find new logfiles and publish those to the observer.onNext
          if (lastProcessedModifiedDate <= DateTimeOffset.MinValue)
          {
            //return all files currently available in the blobstore
            // return all files in the container
            blobContainer.ListBlobs(iisApplicationPrefix, true)
              .OfType<CloudBlockBlob>()
              .OrderBy(blob => blob.Properties.LastModified)
              .Select(blob => new LogFileBlob
              {
                BlockBlob = blob,
                ByteOffsetAlreadyProcessed = 0L,
                TotalBytesAvailableInBlob = blob.Properties.Length
              })
              .ToObservable()
              .Do(newLogFileBlob => lastProcessedModifiedDate = newLogFileBlob.BlockBlob.Properties.LastModified ?? lastProcessedModifiedDate) //keep state of what we have seen
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

              return blobContainer
                .ListBlobs(blobContainerPrefix, true)
                .OfType<CloudBlockBlob>()
                .Where(blob => blob.Properties.LastModified != null && blob.Properties.LastModified > lastProcessedModifiedDate)
                .OrderBy(blob => blob.Properties.LastModified)
                .Select(blob => new LogFileBlob
                {
                  BlockBlob = blob,
                  ByteOffsetAlreadyProcessed = 0L,//TODO this is not correct for subsequent runs
                  TotalBytesAvailableInBlob = blob.Properties.Length
                });
            })
            .SelectMany(date => date)
            .ToObservable()
            .Do(newLogFileBlob => lastProcessedModifiedDate = newLogFileBlob.BlockBlob.Properties.LastModified ?? lastProcessedModifiedDate) //keep state of what we have seen
            .Subscribe(observer);//TODO: does this onComplete the observer stream when the enumeration finishes first iteration?
          }
        });

        return timerObservable;//option to dispose the subscription
      });
    }
  }
}
