package com.retail.offset;
import java.util.concurrent.ConcurrentHashMap;
public class OffsetRepository {
 public static final ConcurrentHashMap<Integer,Long> offsets=new ConcurrentHashMap<>();
}
