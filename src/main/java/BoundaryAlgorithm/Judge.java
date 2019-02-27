package BoundaryAlgorithm;

import java.util.ArrayList;
import java.util.List;

public class Judge {
    public static void main(String[] args) {
        Area a1=new Area(113.941853,22.530777);
        Area a2=new Area(113.940487,22.527789);
        Area a3=new Area(113.94788,22.527597);
        Area a4=new Area(113.947925,22.530618);
        Area a5=new Area(113.941772,22.530727);
        List<Area> areas=new ArrayList<Area>();
        areas.add(a1);
        areas.add(a2);
        areas.add(a3);
        areas.add(a4);
        areas.add(a5);
        Area area = new Area(113.941853,22.530777);
        System.out.println(isInPoint(area,areas));
    }
    public static boolean isInPoint(Area area, List<Area> list){
        for (int i = 0; i < list.size(); i++) {
            if(area.equals(list.get(i))){
                return true;
            }
        }
        return false;
    }
}
