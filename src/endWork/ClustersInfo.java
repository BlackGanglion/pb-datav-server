package endWork;

import java.lang.reflect.Array;
import java.util.List;

import endWork.Cluster;

public class ClustersInfo {
	private int count;
	private String day;
	private String hour;
	private String[] colorList;
	
    private List<Cluster> nodeMap;
    
    public String getDay() {
    	return day;
    }
    
    public String[] getColorList() {
    	return colorList;
    }
    
    public String getHour() {
    	return hour;
    }
	
	public int getCount() {  
		return count;  
    }
	
	public List<Cluster> getNodeMap() {
		return nodeMap;
	}
}
