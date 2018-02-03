package cn.com.google.arithmetic;

/**
 * Created by ThinkPad on 2017/8/22.
 */
public class ClassicsArithmetic {
    public static void main(String[] args) {
        System.out.println(strStr("shellomoses", "es"));

    }

    public static int strStr(String haystack, String needle) {
        if (haystack == null && needle == null) return 0;
        if (haystack == null) return -1;
        if (needle == null) return 0;

        for (int i = 0; i < haystack.length() - needle.length() + 1; i++) {
            int j = 0;
            for (; j < needle.length(); j++) {
                if (haystack.charAt(i + j) != needle.charAt(j)) break;
            }
            if (j == needle.length()) return i;
        }

        return -1;
    }

}
