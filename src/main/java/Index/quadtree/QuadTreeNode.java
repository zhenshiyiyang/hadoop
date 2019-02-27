package Index.quadtree;

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

public class QuadTreeNode {
    public static final int NODE_NUMS = 4;
    public static final int NW = 0, NE = 2, SW = 1, SE = 3;
    /*
     * a quadrant defined below:
     *          NW(0) | NE(2)
     *     -----------|-----------
     *          SW(1) | SE(3)
     */
    public QuadTreeNode[] childNode = new QuadTreeNode[NODE_NUMS];
    public int order;// 四叉树阶数,为20时,精度为19米,21时,精度为2米
    public NodePosition nodeInfo;//保存节点的详细信息

    public QuadTreeNode() {

    }

    public QuadTreeNode(int order) {
        this.order = order;
        nodeInfo = new NodePosition(0, 0, 0, -180, 90, 180, -90, order);
        for(int i=0;i<NODE_NUMS;i++){
            childNode[i]=null;
        }
    }

    public void buildQuadTree(){
        if(this.nodeInfo.depth==this.order)
            return;
        this.createChild();
        for(int i=0;i<NODE_NUMS;i++){
            this.childNode[i].buildQuadTree();
        }
    }

    public void createChild() {
        if(this.nodeInfo.depth==this.order)
            return;
        this.childNode[NW]=initNWNode(this.order);
        this.childNode[NE]=initNENode(this.order);
        this.childNode[SW]=initSWNode(this.order);
        this.childNode[SE]=initSENode(this.order);

        for(int i=0;i<NODE_NUMS;i++){
            this.childNode[i].createChild();
        }
    }

    private void initNodeInfo(int depth, int x_coordinate, int y_coordinate, double upLeft_lon, double upLeft_lat,
                              double lowRight_lon, double lowRight_lat,int order) {
        nodeInfo = new NodePosition(depth, x_coordinate, y_coordinate, upLeft_lon, upLeft_lat, lowRight_lon,
                lowRight_lat,order);
    }

    private QuadTreeNode initNWNode(int order) {
        QuadTreeNode childNode = new QuadTreeNode(order);

        int depth = nodeInfo.depth + 1;
        int x_coordinate = (nodeInfo.x_coordinate << 1) | 0;
        int y_coordinate = (nodeInfo.y_coordinate << 1) | 0;
        double upLeft_lon = nodeInfo.rect.upLeft_lon;
        double upLeft_lat = nodeInfo.rect.upLeft_lat;
        double lowRight_lon = (nodeInfo.rect.lowRight_lon + nodeInfo.rect.upLeft_lon) / 2;
        double lowRight_lat = (nodeInfo.rect.lowRight_lat + nodeInfo.rect.upLeft_lat) / 2;
        childNode.initNodeInfo(depth, x_coordinate, y_coordinate, upLeft_lon, upLeft_lat, lowRight_lon, lowRight_lat,order);
        return childNode;
    }

    private QuadTreeNode initSWNode(int order) {
        QuadTreeNode childNode = new QuadTreeNode(order);

        int depth = nodeInfo.depth + 1;
        int x_coordinate = (nodeInfo.x_coordinate << 1) | 0;
        int y_coordinate = (nodeInfo.y_coordinate << 1) | 1;
        double upLeft_lon = nodeInfo.rect.upLeft_lon;
        double upLeft_lat = (nodeInfo.rect.lowRight_lat + nodeInfo.rect.upLeft_lat) / 2;
        double lowRight_lon = (nodeInfo.rect.lowRight_lon + nodeInfo.rect.upLeft_lon) / 2;
        double lowRight_lat = nodeInfo.rect.lowRight_lat;
        childNode.initNodeInfo(depth, x_coordinate, y_coordinate, upLeft_lon, upLeft_lat, lowRight_lon, lowRight_lat,order);
        return childNode;
    }

    private QuadTreeNode initNENode(int order) {
        QuadTreeNode childNode = new QuadTreeNode(order);

        int depth = nodeInfo.depth + 1;
        int x_coordinate = (nodeInfo.x_coordinate << 1) | 1;
        int y_coordinate = (nodeInfo.y_coordinate << 1) | 0;
        double upLeft_lon = (nodeInfo.rect.lowRight_lon + nodeInfo.rect.upLeft_lon) / 2;
        double upLeft_lat = nodeInfo.rect.upLeft_lat;
        double lowRight_lon = nodeInfo.rect.lowRight_lon;
        double lowRight_lat = (nodeInfo.rect.lowRight_lat + nodeInfo.rect.upLeft_lat) / 2;
        childNode.initNodeInfo(depth, x_coordinate, y_coordinate, upLeft_lon, upLeft_lat, lowRight_lon, lowRight_lat,order);
        return childNode;
    }

    private QuadTreeNode initSENode(int order) {
        QuadTreeNode childNode = new QuadTreeNode(order);

        int depth = nodeInfo.depth + 1;
        int x_coordinate = (nodeInfo.x_coordinate << 1) | 1;
        int y_coordinate = (nodeInfo.y_coordinate << 1) | 1;
        double upLeft_lon = (nodeInfo.rect.lowRight_lon + nodeInfo.rect.upLeft_lon) / 2;
        double upLeft_lat = (nodeInfo.rect.lowRight_lat + nodeInfo.rect.upLeft_lat) / 2;
        double lowRight_lon = nodeInfo.rect.lowRight_lon;
        double lowRight_lat = nodeInfo.rect.lowRight_lat;
        childNode.initNodeInfo(depth, x_coordinate, y_coordinate, upLeft_lon, upLeft_lat, lowRight_lon, lowRight_lat,order);
        return childNode;
    }


}