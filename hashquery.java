import java.nio.ByteBuffer;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.File;
import java.io.RandomAccessFile;

public class hashquery implements dbimpl
{
   private long heapOffset = -1;
   private RandomAccessFile hash = null;
   private RandomAccessFile heap = null;
   private boolean eof;

   // initialize
   public static void main(String args[])
   {
      hashquery load = new hashquery();

      // calculate query time
      long startTime = System.currentTimeMillis();
      load.readArguments(args);
      long endTime = System.currentTimeMillis();

      System.out.println("Query time using hash: " + (endTime - startTime) + "ms");
   }


   // reading command line arguments
   public void readArguments(String args[])
   {
      if (args.length == 2)
      {
         if (isInteger(args[1]))
         {
            readHashFile(args[0], Integer.parseInt(args[1]));
         }
      }
      else
      {
          System.out.println("Error: only pass in two arguments");
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

   public boolean findRecord(String name, long finalOffset, long nextBucketOffset) {
     String line;
     int recNo = 0;
     int bucketNo = 1;
     byte[] record = new byte[hashRecordSize];
     byte[] bName = new byte[BN_NAME_SIZE];
     byte[] offset = new byte[longSize];
     try {
       while(hash.getFilePointer() < finalOffset) {
         if(hash.getFilePointer() == nextBucketOffset) { 
           bucketNo++;
           nextBucketOffset += bucketSize;
         }
         hash.read(record, 0, hashRecordSize);
         recNo++;
         System.arraycopy(record, 0, bName, 0, BN_NAME_SIZE);
         if(ByteBuffer.wrap(bName).getInt() == 0) {
          return true;  
         } 
         else if((new String(bName)).trim().equals(name)) {
           System.arraycopy(record, BN_NAME_SIZE + 1, offset, 0, longSize);
           System.out.println("Buckets visited: " + bucketNo);
           System.out.println("Records scanned until match: " + recNo); 
           heapOffset = ByteBuffer.wrap(offset).getLong();
           return true;
         }
       }
     } catch(Exception e) {
       e.printStackTrace();  
     }
     return false;
   }

   public void readHashFile(String name, int pagesize)
   {
      File hashfile = new File("hash." + pagesize);
      try
      {
         hash = new RandomAccessFile(hashfile, "r");
        
         //put the query into a byte array so that the hash code is the same as in the hash load file
         byte[] query = new byte[BN_NAME_SIZE];
         byte[] temp = name.getBytes();
         System.arraycopy(temp, 0, query, 0, temp.length);

         //hash the name and get the index and the offset
         int hashIndex = new String(query).hashCode() % noOfIndexSlots;
         System.out.println("hash Index: " + hashIndex);
         long hashOffset = hashIndex * bucketSize;
				 hashOffset = (hashOffset < 0) ? hashOffset * -1 : hashOffset;

         //seek to the position in the file
         hash.seek(hashOffset);

         //look for the record starting from the offset until we get to the end of the file
         eof = !findRecord(name, hashFileSize, hashOffset + bucketSize); 

         //if we get to the end of the file without finding the record, and because hashload uses linear probing,
         //then check from the start of the file until we get to our initial bucket
         if(eof) {
          //reset the pointer to the start of the file
          hash.seek(0);  
          findRecord(name, hashOffset, hashOffset + bucketSize);
         }

         //if a record in the hash file was found, find it in the heap file and print it to the console
         if(heapOffset >= 0) {
           byte[] heapRecord = new byte[RECORD_SIZE];
           heap = new RandomAccessFile("heap." + pagesize, "r");
           heap.seek(heapOffset);
           heap.read(heapRecord, 0, RECORD_SIZE);
           System.out.println(new String(heapRecord));
         } else {
           System.out.println("No matching record");  
         }
      }
      catch (FileNotFoundException e)
      {
         System.out.println("File: " + "hash." + pagesize + " not found.");
      }
      catch (IOException e)
      {
         e.printStackTrace();
      } finally {
        //close the streams
        try {
          if(hash != null) {
            hash.close();
          }
          if(heap != null) {
            heap.close();
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
   }
}
