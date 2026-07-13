package com.retail.offset;
public class OffsetManager{
 public static void commit(int p,long o){ OffsetRepository.offsets.put(p,o); System.out.println("COMMIT "+p+" "+o);}
}
