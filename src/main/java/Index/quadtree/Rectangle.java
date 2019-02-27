package Index.quadtree;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeSet;

public class Rectangle {
    /*
     * lon:-180~180
     * lat:-90~90
     */
    double upLeft_lon, upLeft_lat;
    double lowRight_lon, lowRight_lat;
    double width,height;
    public int depth;//树深
    public TreeSet<String> codeList=new TreeSet<>();//存储所要查询区域的Hilbert编码(二进制)

    public Rectangle(){

    }

    public Rectangle(double upLeft_lon, double upLeft_lat,
                     double lowRight_lon, double lowRight_lat) {
        this.upLeft_lon = upLeft_lon;
        this.upLeft_lat = upLeft_lat;
        this.lowRight_lon = lowRight_lon;
        this.lowRight_lat = lowRight_lat;
        width=Math.abs(lowRight_lon-upLeft_lon);
        height=Math.abs(upLeft_lat-lowRight_lat);
    }
    public Rectangle(double upLeft_lon, double upLeft_lat,
                     double lowRight_lon, double lowRight_lat, int depth) {
        this.upLeft_lon = upLeft_lon;
        this.upLeft_lat = upLeft_lat;
        this.lowRight_lon = lowRight_lon;
        this.lowRight_lat = lowRight_lat;
        width=Math.abs(lowRight_lon-upLeft_lon);
        height=Math.abs(upLeft_lat-lowRight_lat);
        this.depth=depth;
    }

    public boolean contains(Rectangle r){
        Rectangle rect1=new Rectangle(this.upLeft_lon+180, -this.upLeft_lat+90, this.lowRight_lon+180, -this.lowRight_lat+90);//四叉树区域
        Rectangle rect2=new Rectangle(r.upLeft_lon+180, -r.upLeft_lat+90, r.lowRight_lon+180, -r.lowRight_lat+90);//查询区域
        double w = rect1.width;
        double h = rect1.height;
        double W = rect2.width;
        double H = rect2.height;
        if (w < 0||h < 0||W < 0||H < 0) {
            // At least one of the dimensions is negative...
            return false;
        }
        // Note: if any dimension is zero, tests below must return false...
        double x = rect1.upLeft_lon;
        double y = rect1.upLeft_lat;
        double X = rect2.upLeft_lon;
        double Y = rect2.upLeft_lat;
        if (X < x || Y < y) {
            return false;
        }
        w += x;
        W += X;
        if (W <= X) {
            // X+W overflowed or W was zero, return false if...
            // either original w or W was zero or
            // x+w did not overflow or
            // the overflowed x+w is smaller than the overflowed X+W
            if (w >= x || W > w) return false;
        } else {
            // X+W did not overflow and W was not zero, return false if...
            // original w was zero or
            // x+w did not overflow and x+w is smaller than X+W
            if (w >= x && W > w) return false;
        }
        h += y;
        H += Y;
        if (H <= Y) {
            if (h >= y || H > h) return false;
        } else {
            if (h >= y && H > h) return false;
        }
        return true;
    }

    public boolean intersects(Rectangle r) {
        Rectangle rect1=new Rectangle(this.upLeft_lon+180, -this.upLeft_lat+90, this.lowRight_lon+180, -this.lowRight_lat+90);//�Ĳ�������
        Rectangle rect2=new Rectangle(r.upLeft_lon+180, -r.upLeft_lat+90, r.lowRight_lon+180, -r.lowRight_lat+90);//��ѯ����
        double tw = rect1.width;
        double th = rect1.height;
        double rw = rect2.width;
        double rh = rect2.height;
        if (rw <= 0 || rh <= 0 || tw <= 0 || th <= 0) {
            return false;
        }
        double tx = rect1.upLeft_lon;
        double ty = rect1.upLeft_lat;
        double rx = rect2.upLeft_lon;
        double ry = rect2.upLeft_lat;
        rw += rx;
        rh += ry;
        tw += tx;
        th += ty;
        //      overflow || intersect
        return ((rw < rx || rw > tx) &&
                (rh < ry || rh > ty) &&
                (tw < tx || tw > rx) &&
                (th < ty || th > ry));
    }

    public Rectangle intersection(Rectangle r) {
        Rectangle rect1=new Rectangle(this.upLeft_lon+180, -this.upLeft_lat+90, this.lowRight_lon+180, -this.lowRight_lat+90);//�Ĳ�������
        Rectangle rect2=new Rectangle(r.upLeft_lon+180, -r.upLeft_lat+90, r.lowRight_lon+180, -r.lowRight_lat+90);//��ѯ����
        double tx1 = rect1.upLeft_lon;
        double ty1 = rect1.upLeft_lat;
        double rx1 = rect2.upLeft_lon;
        double ry1 = rect2.upLeft_lat;
        double tx2 = tx1; tx2 += rect1.width;
        double ty2 = ty1; ty2 += rect1.height;
        double rx2 = rx1; rx2 += rect2.width;
        double ry2 = ry1; ry2 += rect2.height;
        if (tx1 < rx1) tx1 = rx1;
        if (ty1 < ry1) ty1 = ry1;
        if (tx2 > rx2) tx2 = rx2;
        if (ty2 > ry2) ty2 = ry2;
        tx2 -= tx1;
        ty2 -= ty1;
        // tx2,ty2 will never overflow (they will never be
        // larger than the smallest of the two source w,h)
        // they might underflow, though...
        if (tx2 < Double.MIN_VALUE) tx2 = Double.MIN_VALUE;
        if (ty2 < Double.MIN_VALUE) ty2 = Double.MIN_VALUE;
        return new Rectangle(tx1-180, -ty1+90, tx1-180+tx2, -ty1+90-ty2);
    }
    /*判断四叉树每个节点分隔的区域是否和想要查询的区域相交或是包含*/
    public void geometryQuery(Rectangle rect,QuadTreeNode node){
        /*包含*/
        if(node!=null&&rect.contains(node.nodeInfo.rect)){
            codeList.add(node.nodeInfo.toBinaryCode1());
            return;
        }
        /*相交*/
        if(node!=null&&node.nodeInfo.depth==node.order&&node.nodeInfo.rect.intersects(rect)){
            codeList.add(node.nodeInfo.toBinaryCode1());
            return;
        }
        /*判断下一层节点是否相交*/
        if(node.nodeInfo.rect.intersects(rect)){
            for(int i=0;i<4;i++){
                if(node.childNode[i].nodeInfo.rect.intersects(rect)){
                    Rectangle rect1=node.childNode[i].nodeInfo.rect.intersection(rect);
                    geometryQuery(rect1,node.childNode[i]);
                }
            }

        }

    }

}
