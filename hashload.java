import java.nio.ByteBuffer;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.*;

public class hashload implements dbimpl
{
   // initialize
   public static void main(String args[])
   {
      hashload load = new hashload();

      // calculate time to create hash file
      long startTime = System.currentTimeMillis();
      load.readArguments(args);
      long endTime = System.currentTimeMillis();

      System.out.println("Time taken to create hashfile: " + (endTime - startTime) + "ms");
   }


   // reading command line arguments
   public void readArguments(String args[])
   {
      if (args.length == 1)
      {
         if (isInteger(args[0]))
         {
            readHeap(Integer.parseInt(args[0]));
         }
      }
      else
      {
          System.out.println("Error: only pass in one arguments");
      }
   }

   // check if pagesize is a valid integer
   public boolean isInteger(String s)
   {
      boolean isValidInt = false;
      try
      {
         Integer.parseInt(s);
         isValidInt = true;
      }
      catch (NumberFormatException e)
      {
         e.printStackTrace();
      }
      return isValidInt;
   }

   // read heapfile by page
   public void readHeap(int pagesize)
   {
      File heapfile = new File(HEAP_FNAME + pagesize);
      int intSize = 4;
      int pageCount = 0;
      int recCount = 0;
      int recordLen = 0;
      int rid = 0;
      int hashIndex = 0;
      boolean isNextPage = true;
      boolean isNextRecord = true;

      //this list stores each bucket
      ArrayList<byte[]> buckets = new ArrayList<byte[]>(noOfIndexSlots);
      for(int i = 0; i < noOfIndexSlots; ++i) {
        byte[] b = new byte[bucketSize];
        buckets.add(b);
      }

      //this array keeps track of the size of each bucket so we can insert easily
      int[] currBucketSize = new int[noOfIndexSlots];

      FileInputStream fis = null;
      FileOutputStream fos = null;

      try
      {
         fis = new FileInputStream(heapfile);
         fos = new FileOutputStream("hash." + pagesize);
 
         // reading page by page
         while (isNextPage)
         {
            byte[] bPage = new byte[pagesize];
            byte[] bPageNum = new byte[intSize];
            fis.read(bPage, 0, pagesize);
            System.arraycopy(bPage, bPage.length-intSize, bPageNum, 0, intSize);

            // reading by record, return true to read the next record
            isNextRecord = true;
            while (isNextRecord)
            {
               byte[] bRecord = new byte[RECORD_SIZE];
               byte[] bRid = new byte[intSize];
               byte[] bName = new byte[BN_NAME_SIZE];
               //byte array for storing the hash record
               byte[] hashRecord = new byte[hashRecordSize];
               byte[] offset;
               try
               {
                  System.arraycopy(bPage, recordLen, bRecord, 0, RECORD_SIZE);
                  System.arraycopy(bRecord, 0, bRid, 0, intSize);
                  System.arraycopy(bRecord, BN_NAME_OFFSET, bName, 0, BN_NAME_SIZE);
                  //copy the name into the hash record byte array
                  System.arraycopy(bName, 0, hashRecord, 0, BN_NAME_SIZE);
                  hashRecord[BN_NAME_SIZE] = delim;

                  //get the offset in the heap file and copy it into the hash record byte array
                  long offsetVal = pageCount * pagesize + recCount * RECORD_SIZE;
                  offset = ByteBuffer.allocate(longSize).putLong(offsetVal).array();
                  System.arraycopy(offset, 0, hashRecord, BN_NAME_SIZE + charSize, longSize);
                  hashRecord[hashRecordSize-1] = lineFeed;

                  //get a hash index based off of the records name
                  hashIndex = new String(bName).hashCode() % noOfIndexSlots;
                  hashIndex = (hashIndex < 0) ? hashIndex * -1 : hashIndex;
                  
                  //if the bucket does not have enough space, find the next one that does
                  //if there is not enough space left in the hash file, write out what fits
                  while(true) {
                    if(hashIndex >= noOfIndexSlots) {
                      System.out.println("Hash file too small, change the variables in dbimp, file does not contain all records");
                      hashIndex = -1;
											break;
                    } else if(currBucketSize[hashIndex] == bucketSize) {
                      hashIndex++;  
                    } else {
                      break;  
                    }
                  }

                  rid = ByteBuffer.wrap(bRid).getInt();
                  if (rid != recCount)
                  {
                     isNextRecord = false;
                  } else if(hashIndex < 0)
                  {
                     break;
                  }
                  else
                  {
                     //insert into the record into the right bucket
                     System.arraycopy(hashRecord, 0, buckets.get(hashIndex), currBucketSize[hashIndex], hashRecordSize);
                     currBucketSize[hashIndex] += hashRecordSize;
                     recordLen += RECORD_SIZE;
                  }
                  recCount++;
                  // if recordLen exceeds pagesize, catch this to reset to next page
               }
               catch (ArrayIndexOutOfBoundsException e)
               {
                  isNextRecord = false;
                  recordLen = 0;
                  recCount = 0;
                  rid = 0;
               }
            }
            // check to complete all pages
            if (ByteBuffer.wrap(bPageNum).getInt() != pageCount)
            {
               isNextPage = false;
            }
            pageCount++;
            if(hashIndex < 0) break;
         }
         //write out the hash file using the buckets
         for(int j = 0; j < buckets.size(); ++j) {
           fos.write(buckets.get(j));  
         }

      }
      catch (FileNotFoundException e)
      {
         System.out.println("File: " + HEAP_FNAME + pagesize + " not found.");
      }
      catch (IOException e)
      {
         e.printStackTrace();
      } finally {
        //close the streams
        try {
          if(fis != null) {
            fis.close();
          }
          if(fos != null) {
            fos.close();
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
   }
}
