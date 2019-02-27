package Index.quadtree;

import java.util.HashMap;
import java.util.Map;



/*
hilbert_map = {
'a': {(0, 0): (0, 'd'), (0, 1): (1, 'a'), (1, 0): (3, 'b'), (1, 1): (2, 'a')},
'b': {(0, 0): (2, 'b'), (0, 1): (1, 'b'), (1, 0): (3, 'a'), (1, 1): (0, 'c')},
'c': {(0, 0): (2, 'c'), (0, 1): (3, 'd'), (1, 0): (1, 'c'), (1, 1): (0, 'b')},
'd': {(0, 0): (0, 'a'), (0, 1): (3, 'c'), (1, 0): (1, 'd'), (1, 1): (2, 'd')}}

def point_to_hilbert(x, y, order=16):
    current_square = 'a'
    position = 0
    for i in range(order - 1, -1, -1):
        position <<= 2
        quad_x = 1 if x & (1 << i) else 0
        quad_y = 1 if y & (1 << i) else 0
        quad_position, current_square = hilbert_map[current_square][(quad_x, quad_y)]
        position |= quad_position
    return position
 *
*/
public class HilbertCode {
    public static final double lonMax=180;
    public static final double lonMin=-180;
    public static final double latMax=90;
    public static final double latMin=-90;

    public static Map<coordKey,Value>[] hilbert_map=new HashMap[4];

    static class coordKey{
        int x;
        int y;
        public coordKey(int x, int y) {
            this.x = x;
            this.y = y;
        }
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + x;
            result = prime * result + y;
            return result;
        }
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            coordKey other = (coordKey) obj;
            if (x != other.x)
                return false;
            if (y != other.y)
                return false;
            return true;
        }
    }

    static class Value{
        int code;
        int position;
        public Value(int code,int position){
            this.code=code;
            this.position=position;
        }
    }

    static{
        hilbert_map[0]=new HashMap<coordKey,Value>();
        hilbert_map[0].put(new coordKey(0,0),new Value(0, 3)); hilbert_map[0].put(new coordKey(0,1),new Value(1, 0));
        hilbert_map[0].put(new coordKey(1,0),new Value(3, 1)); hilbert_map[0].put(new coordKey(1,1),new Value(2, 0));

        hilbert_map[1]=new HashMap<coordKey,Value>();
        hilbert_map[1].put(new coordKey(0,0),new Value(2, 1)); hilbert_map[1].put(new coordKey(0,1),new Value(1, 1));
        hilbert_map[1].put(new coordKey(1,0),new Value(3, 0)); hilbert_map[1].put(new coordKey(1,1),new Value(0, 2));

        hilbert_map[2]=new HashMap<coordKey,Value>();
        hilbert_map[2].put(new coordKey(0,0),new Value(2, 2)); hilbert_map[2].put(new coordKey(0,1),new Value(3, 3));
        hilbert_map[2].put(new coordKey(1,0),new Value(1, 2)); hilbert_map[2].put(new coordKey(1,1),new Value(0, 1));

        hilbert_map[3]=new HashMap<coordKey,Value>();
        hilbert_map[3].put(new coordKey(0,0),new Value(0, 0)); hilbert_map[3].put(new coordKey(0,1),new Value(3, 2));
        hilbert_map[3].put(new coordKey(1,0),new Value(1, 3)); hilbert_map[3].put(new coordKey(1,1),new Value(2, 3));

    }
    public static int point_to_hilbert(int x, int y, int order){
        int current_square=0;
        int hilbert_code=0;
        int quad_code=0;
        int quad_x=0,quad_y=0;
        for(int i=order-1;i>=0;i--){
            hilbert_code<<=2;
            if((x&(1<<i))==0)
                quad_x=0;
            else
                quad_x=1;
            if((y&(1<<i))==0)
                quad_y=0;
            else
                quad_y=1;
            quad_code=hilbert_map[current_square].get(new coordKey(quad_x, quad_y)).code;
            current_square=hilbert_map[current_square].get(new coordKey(quad_x, quad_y)).position;
            hilbert_code|=quad_code;
        }
        return hilbert_code;
    }


/*	public static int lonLat_to_hilbert(double lon,double lat,int order){
		int hilbert_code=0;
		int x=0,y=0;
		double x_max=lon*Math.pow(2, order)/(lonMax-lonMin);
		double y_max=(latMax-lat)*Math.pow(2, order)/(latMax-latMin);
		x=(int)Math.floor(x_max);
		y=(int)Math.floor(y_max);
		hilbert_code=point_to_hilbert(x,y,order);
		return hilbert_code;
	}*/

}