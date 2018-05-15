/**
 *  Database Systems - HEAP IMPLEMENTATION
 */

public interface dbimpl
{

   public static final String HEAP_FNAME = "heap.";
   public static final String ENCODING = "utf-8";

   // fixed/variable lengths
   public static final int RECORD_SIZE = 297;
   public static final int RID_SIZE = 4;
   public static final int REGISTER_NAME_SIZE = 14;
   public static final int BN_NAME_SIZE = 200;
   public static final int BN_STATUS_SIZE = 12;
   public static final int BN_REG_DT_SIZE = 10;
   public static final int BN_CANCEL_DT_SIZE = 10;
   public static final int BN_RENEW_DT_SIZE = 10;
   public static final int BN_STATE_NUM_SIZE = 10;
   public static final int BN_STATE_OF_REG_SIZE = 3;
   public static final int BN_ABN_SIZE = 20;
   public static final int EOF_PAGENUM_SIZE = 4;

	 //these variables are used to determine the size of a bucket
   //a record is made up of a business name, a delimiter, a page offset, and a line feed
   public static final int longSize = 8;
   public static final int charSize = 1;
   public static final int hashRecordSize = BN_NAME_SIZE + charSize + longSize + charSize;
   public static final int noOfRecordsInBucket = 2;
   public static final int bucketSize = hashRecordSize * noOfRecordsInBucket;
   public static final int noOfIndexSlots = 710000;
   public static final int hashFileSize = bucketSize * noOfIndexSlots;
	 public static final int delim = 124;
   public static final int lineFeed = 10;

   public static final int BN_NAME_OFFSET = RID_SIZE
                           + REGISTER_NAME_SIZE;

   public static final int BN_STATUS_OFFSET = RID_SIZE
                           + REGISTER_NAME_SIZE
                           + BN_NAME_SIZE;

   public static final int BN_REG_DT_OFFSET = RID_SIZE
                           + REGISTER_NAME_SIZE
                           + BN_NAME_SIZE
                           + BN_STATUS_SIZE;

   public static final int BN_CANCEL_DT_OFFSET = RID_SIZE
                           + REGISTER_NAME_SIZE
                           + BN_NAME_SIZE
                           + BN_STATUS_SIZE
                           + BN_REG_DT_SIZE;

   public static final int BN_RENEW_DT_OFFSET = RID_SIZE
                           + REGISTER_NAME_SIZE
                           + BN_NAME_SIZE
                           + BN_STATUS_SIZE
                           + BN_REG_DT_SIZE
                           + BN_CANCEL_DT_SIZE;

   public static final int BN_STATE_NUM_OFFSET = RID_SIZE
                           + REGISTER_NAME_SIZE
                           + BN_NAME_SIZE
                           + BN_STATUS_SIZE
                           + BN_REG_DT_SIZE
                           + BN_CANCEL_DT_SIZE
                           + BN_RENEW_DT_SIZE;

   public static final int BN_STATE_OF_REG_OFFSET = RID_SIZE
                           + REGISTER_NAME_SIZE
                           + BN_NAME_SIZE
                           + BN_STATUS_SIZE
                           + BN_REG_DT_SIZE
                           + BN_CANCEL_DT_SIZE
                           + BN_RENEW_DT_SIZE
                           + BN_STATE_NUM_SIZE;

   public static final int BN_ABN_OFFSET = RID_SIZE
                           + REGISTER_NAME_SIZE
                           + BN_NAME_SIZE
                           + BN_STATUS_SIZE
                           + BN_REG_DT_SIZE
                           + BN_CANCEL_DT_SIZE
                           + BN_RENEW_DT_SIZE
                           + BN_STATE_NUM_SIZE
                           + BN_STATE_OF_REG_SIZE;

   public void readArguments(String args[]);

   public boolean isInteger(String s);

}
