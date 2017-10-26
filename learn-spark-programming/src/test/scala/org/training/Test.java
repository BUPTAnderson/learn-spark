package org.training;

import java.io.File;

/**
 * Created by anderson on 17-10-17.
 */
public class Test
{
    public static void main(String[] args)
    {
        System.out.println(File.separator);
        String[] strs = "abc de de".split("\\s+");
        for (String str : strs) {
            System.out.println(str);
        }
    }
}
