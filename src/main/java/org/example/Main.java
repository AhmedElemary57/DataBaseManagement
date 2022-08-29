package org.example;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

public class Main {
    public static void main(String[] args)
    {
        BloomFilter<String> bloomFilter = BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_16), 100, 0.01);
        bloomFilter.put("hello");
        bloomFilter.put("hi");
        String testString = "india";
        boolean checkStatus = bloomFilter.mightContain(testString);
        System.out.println(testString + (checkStatus?" seen before":" not seen before"));
        testString = "hello";
        checkStatus = bloomFilter.mightContain(testString);
        System.out.println(testString + (checkStatus?" seen before":" not seen before"));
    }
}