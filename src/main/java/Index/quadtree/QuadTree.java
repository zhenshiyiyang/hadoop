package Index.quadtree;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.TreeSet;


public class QuadTree {
    public QuadTreeNode root;//四叉树的根节点
    public int depth;//树深
    public TreeSet<String> codeList=new TreeSet<>();//存储所要查询区域的Hilbert编码(二进制)

    public QuadTree(QuadTreeNode root, int depth) {
        this.root = root;
        this.depth = depth;
    }

    public void buildQuadTree(){
        root.createChild();
    }

    public void geometryQuery(Rectangle rect,QuadTreeNode node){
        codeList.clear();//清除上次查询保留的结果
        spatialQuery(rect,node);
    }


    public void spatialQuery(Rectangle rect,QuadTreeNode node){
        if(node!=null&&rect.contains(node.nodeInfo.rect)){
            codeList.add(node.nodeInfo.toBinaryCode1());
            return;
        }
        if(node!=null&&node.nodeInfo.depth==this.depth&&node.nodeInfo.rect.intersects(rect)){
            codeList.add(node.nodeInfo.toBinaryCode1());
            return;
        }
        if(node.nodeInfo.rect.intersects(rect)){
            for(int i=0;i<4;i++){
                if(node.childNode[i].nodeInfo.rect.intersects(rect)){
                    Rectangle rect1=node.childNode[i].nodeInfo.rect.intersection(rect);
                    geometryQuery(rect1,node.childNode[i]);
                }
            }
        }
    }

    public void printTree(){
        Queue<QuadTreeNode> queue=new LinkedList<QuadTreeNode>();
        int level=0;
        int order=0;
        if(root!=null){
            queue.add(root);
            while(!queue.isEmpty()){
                QuadTreeNode node=queue.poll();
                if(order==0){
                    System.out.println("--------------------第"+node.nodeInfo.depth+"层-------------------");
                }
                System.out.println("层号:"+node.nodeInfo.depth+" "+"坐标X:"+node.nodeInfo.x_coordinate+"("+Integer.toBinaryString(node.nodeInfo.x_coordinate)+") "
                        +"坐标Y:"+node.nodeInfo.y_coordinate+"("+Integer.toBinaryString(node.nodeInfo.y_coordinate)+")"+" "+node.nodeInfo.nodeCode+" "+node.nodeInfo.binaryCode+" "
                        +"("+node.nodeInfo.rect.upLeft_lon+","+node.nodeInfo.rect.upLeft_lat+")"+"("+node.nodeInfo.rect.lowRight_lon+","+node.nodeInfo.rect.lowRight_lat+")");
                level++;
                if(level%Math.pow(4, order)==0){
                    System.out.println("--------------------第"+(node.nodeInfo.depth+1)+"层-------------------");
                    level=0;
                    order++;
                }
                for(int i=0;i<4;i++){
                    if(node.childNode[i]!=null){
                        queue.add(node.childNode[i]);
                    }
                }
            }
        }
    }
}
