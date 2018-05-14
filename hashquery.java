import java.nio.ByteBuffer;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.File;
import java.io.RandomAccessFile;

public class hashquery implements dbimpl
{
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

   public void readHashFile(String name, int pagesize)
   {
      File hashfile = new File("hash." + pagesize);
      long heapOffset = -1;

      try
      {
         RandomAccessFile hash = new RandomAccessFile(hashfile, "r");
        
         //put the query into a byte array so that the hash code is the same as in the hash load file
         byte[] query = new byte[BN_NAME_SIZE];
         byte[] temp = name.getBytes();
         System.arraycopy(temp, 0, query, 0, temp.length);

         //hash the name and get the index and the offset
         int hashIndex = new String(query).hashCode() % noOfIndexSlots;
         System.out.println(hashIndex);
         long hashOffset = hashIndex * bucketSize;
				 hashOffset = (hashOffset < 0) ? hashOffset * -1 : hashOffset;

         //seek to the position in the file
         hash.seek(hashOffset);

         //read line by line until you find the matching record
         String line;
         byte[] record = new byte[hashRecordSize];
         byte[] bName = new byte[BN_NAME_SIZE];
         byte[] offset = new byte[longSize];
         while(true) {
           hash.read(record, 0, hashRecordSize);
           System.arraycopy(record, 0, bName, 0, BN_NAME_SIZE);
           System.out.println(new String(bName));
           if(ByteBuffer.wrap(bName).getInt() == 0) {
            break;  
           } else if((new String(bName)).trim().equals(name)) {
             System.arraycopy(record, BN_NAME_SIZE + 1, offset, 0, longSize);
             heapOffset = ByteBuffer.wrap(offset).getLong();
             break;
           }
         }

         //if a record in the hash file was found, find it in the heap file and print it to the console
         if(heapOffset > 0) {
           byte[] heapRecord = new byte[RECORD_SIZE];
           RandomAccessFile heap = new RandomAccessFile("heap." + pagesize, "r");
           heap.seek(heapOffset);
           heap.read(heapRecord, 0, RECORD_SIZE);
           System.out.println(new String(heapRecord));
         }
      }
      catch (FileNotFoundException e)
      {
         System.out.println("File: " + "hash." + pagesize + " not found.");
      }
      catch (IOException e)
      {
         e.printStackTrace();
      }
   }
}
