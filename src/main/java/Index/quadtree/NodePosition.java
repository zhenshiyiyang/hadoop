package Index.quadtree;

public class NodePosition{
    public int depth;//所处深度
    public int CODE_LENGTH;//叶子节点的Hilbert编码长度
    public int x_coordinate;
    public int y_coordinate;
    /*	public double upLeft_lon;//左上角经度
        public double upLeft_lat;//左上角纬度
        public double lowRight_lon;//右下角经度
        public double lowRight_lat;//右下角纬度  	*/
    Rectangle rect;//节点的位置信息
    public long nodeCode;//Hilbert编码
    public String binaryCode;//二进制编码

    public NodePosition(int depth, int x_coordinate, int y_coordinate, double upLeft_lon, double upLeft_lat,
                        double lowRight_lon, double lowRight_lat,int order) {
        this.depth = depth;
        this.x_coordinate = x_coordinate;
        this.y_coordinate = y_coordinate;
/*		this.upLeft_lon = upLeft_lon;
		this.upLeft_lat = upLeft_lat;
		this.lowRight_lon = lowRight_lon;
		this.lowRight_lat = lowRight_lat;*/
        rect=new Rectangle(upLeft_lon, upLeft_lat, lowRight_lon, lowRight_lat);
        CODE_LENGTH=2*order;
        this.nodeCode=HilbertCode.point_to_hilbert(x_coordinate, y_coordinate, depth);
        this.binaryCode=toBinaryCode1();
        //this.binaryCode=Long.toBinaryString(this.nodeCode);
    }

    public NodePosition(){

    }

    //按当前节点所在的深度来归化编码的位数
    public String toBinaryCode1(){
        String binary=Long.toBinaryString(this.nodeCode);
        if(binary.length()<this.depth*2){
            int i=2*this.depth-binary.length();
            while(i>0){
                binary="0"+binary;
                i--;
            }
        }
        return binary;
    }

	/*//按四叉树的阶数来归化编码的位数
	public String toBinaryCode(){
		String binary=Long.toBinaryString(this.nodeCode);
		if(binary.length()<CODE_LENGTH){
			int i=CODE_LENGTH-binary.length();
			while(i>0){
				binary+="0";
				i--;
			}
		}
		return binary;
	}*/
}
