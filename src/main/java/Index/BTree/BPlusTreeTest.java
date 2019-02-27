package Index.BTree;
import Index.BTree.BPlusTree.RangePolicy;

import java.util.ArrayList;

public class BPlusTreeTest {
    public static void main(String[] args) {
        BPlusTree<String, ArrayList<String>> bpt = new BPlusTree<String, ArrayList<String>>(4);
        ArrayList<String> a = new ArrayList<String>();
        a.add("1");a.add("2");a.add("3");
        ArrayList<String> b = new ArrayList<String>();
        b.add("2");
        b.add("3");
        bpt.insert("20170101", a);
        bpt.insert("20170102", b);
        BPlusTree<String, String> bpt1 = new BPlusTree<String, String>(4);
        bpt1.insert("20170103", "1");
        bpt1.insert("20170103", "2");
        /*已有key值插入新value值*/
        bpt.search("20170101").add("4");
        System.out.println(bpt.search("20170101"));
        System.out.println(bpt.search("20170102"));
        System.out.println(bpt.searchRange("20170101",RangePolicy.INCLUSIVE,"20170102",RangePolicy.INCLUSIVE));
        System.out.println(bpt.search("20170104"));
        System.out.println(bpt1.search("20170103"));
    }
}

