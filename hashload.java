import java.nio.ByteBuffer;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.File;
import java.io.FileInputStream;
import java.util.*;

public class hashload implements dbimpl
{
   // initialize
   public static void main(String args[])
   {
      hashload load = new hashload();

      // calculate query time
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
      boolean isNextPage = true;
      boolean isNextRecord = true;

      //these variables are used to determine the size of a bucket
      //a record is made up of a business name, a delimiter, a page offset, and a line feed
      int longSize = 8;
      int charSize = 1;
      int recordSize = BN_NAME_SIZE + charSize + longSize + charSize;
      int noOfRecordsInBucket = 3;
      int bucketSize = recordSize * noOfRecordsInBucket;
      int hashIndexSlots = 10000;

      //this list stores each bucket
      ArrayList<byte[]> buckets = new ArrayList<byte[]>(hashIndexSlots);
      for(int i = 0; i < buckets.size(); ++i) {
        byte[] bucket = new byte[bucketSize];
        buckets.add(bucket);
      }

      try
      {
         FileInputStream fis = new FileInputStream(heapfile);
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
               //byte array for storing the name
               byte[] bName = new byte[BN_NAME_SIZE];
               try
               {
                  System.arraycopy(bPage, recordLen, bRecord, 0, RECORD_SIZE);
                  System.arraycopy(bRecord, 0, bRid, 0, intSize);
                  //copy the name from the record into the name byte array
                  System.arraycopy(bRecord, BN_NAME_OFFSET, bName, 0, BN_NAME_SIZE);
                  
                  //get a hash index based off of the records name
                  int hashIndex = new String(bName).hashCode() % 10000;

                  System.out.println(hashIndex);

                  rid = ByteBuffer.wrap(bRid).getInt();
                  if (rid != recCount)
                  {
                     isNextRecord = false;
                  }
                  else
                  {
                     System.out.println(new String(bName));
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
         }
      }
      catch (FileNotFoundException e)
      {
         System.out.println("File: " + HEAP_FNAME + pagesize + " not found.");
      }
      catch (IOException e)
      {
         e.printStackTrace();
      }
   }
}
