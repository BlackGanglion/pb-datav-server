package endWork;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.swing.text.html.HTMLDocument.Iterator;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.databind.DeserializationFeature;

import endWork.ClustersInfo;
import endWork.Cluster;

/**
 * Servlet implementation class CalCluster
 */
@WebServlet("/CalCluster.json")
public class CalCluster extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public CalCluster() {
        super();
        // TODO Auto-generated constructor stub
    }
    
	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		response.getWriter().append("Served at: ").append(request.getContextPath());
	}
	
	public static boolean isInt(String string) {
        return string.matches("\\d+");
    }

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		
		BufferedReader br = new BufferedReader(new InputStreamReader((ServletInputStream) request.getInputStream(), "utf-8"));
		StringBuffer sb = new StringBuffer("");
		String temp;
		while ((temp = br.readLine()) != null) {
			sb.append(temp);
		}
		br.close();
		String params = sb.toString();
		
		ClustersInfo clustersInfo = mapper.readValue(params, ClustersInfo.class);
		
		// 有多少区域
		// int count = clustersInfo.getCount();
		String day = clustersInfo.getDay();
		String hour = clustersInfo.getHour();
		// 所有id对应区域的map
		List<Cluster> nodeMap = clustersInfo.getNodeMap();
		String[] colorList = clustersInfo.getColorList();
		
		int count = colorList.length;
		
		List map[][] = new List[count + 10][count + 10];
		
		// 建图
		for (int i = 0; i < count; i++) {
			for (int j = 0; j < count; j++) {
				map[i][j] = new ArrayList();
			}
		}
		
		JSONObject res = new JSONObject();
		
		// 建立点到区域的映射关系
		// k -> cid
		Map cidMap1 = new HashMap();
		// cid -> k
		Map cidMap2 = new HashMap();
		// id -> k
		Map nodeIdMap = new HashMap();
		JSONArray nodes = new JSONArray();
		int k = 0;
		for(int i = 0; i < nodeMap.size(); i++)  {  
			Cluster item = nodeMap.get(i);
			
			// 节点id
			int id = item.getId();
			// 站点id
			int cid = item.getClusterId();
			
			// 当前区域是否已被定义
			Object index = cidMap2.get(cid);
			if (index == null) {
				cidMap1.put(k, cid);
				cidMap2.put(cid, k);
				nodeIdMap.put(id, k);
				k++;
			} else {
				nodeIdMap.put(id, (int)index);
			}
        }
		
		Connection con = connect.dbConn();
		
		try {
			if (con == null) {
				System.out.print("connect failed");
	        }
			System.out.print("connect success");
			
			Long t1 = System.currentTimeMillis();
			
			Statement sql = con.createStatement();
			
			ResultSet result;
			if (hour != null) {
				String sqlHour = "SELECT * FROM B_LEASEINFOHIS_SUM_PART partition(D" + day + ")"
						+ " WHERE LEASETIME = " + hour;
				result = sql.executeQuery(sqlHour);
			} else {
				String sqlNoHour = "SELECT * FROM B_LEASEINFOHIS_SUM_PART partition(D" + day + ")";
				result = sql.executeQuery(sqlNoHour);
			}
			
			result.setFetchSize(100000);
			int i = 1;
			while (result.next()) {
				String sStr = result.getString(3);
				String tStr = result.getString(4);
				// 去除杂质数据
				if (isInt(sStr) && isInt(tStr)) {
					int source = Integer.parseInt(sStr);
					int target = Integer.parseInt(tStr);
					
					// System.out.println(source);
					// System.out.println(target);
					
					String time = result.getString(2);
					int bikeNum = result.getInt(5);
					Object sourceCid = nodeIdMap.get(source);
					Object targetCid = nodeIdMap.get(target);
					
					// 去除无关数据与区域内部数据
					if (sourceCid != null && targetCid != null && sourceCid != targetCid) {
						map[(int) sourceCid][(int) targetCid].add(new String(sStr + "," + tStr + "," + time + "," + bikeNum));
					}
				}
			}
			
			Long t2 = System.currentTimeMillis();
        	System.out.println(t2 - t1);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		JSONArray links = new JSONArray();
		for(int i = 0; i < count; i++) {
			for(int j = i + 1; j < count; j++) {
				int l1 = map[i][j].size();
				int l2 = map[j][i].size();
				
				if (l1 > 0 || l2 > 0) {
					JSONArray relations = new JSONArray();
					int allNum = 0;
					
					int scid = (int)cidMap1.get(i);
					int tcid = (int)cidMap1.get(j);
					
					for(int z = 0; z < l1; z++) {
						String[] info = ((String) map[i][j].get(z)).split(",");
						String source = info[0];
						String target = info[1];
						String time = info[2];
						int num = Integer.parseInt(info[3]);
						allNum += num;
						
						JSONObject relation = new JSONObject();
						relation.put("hour", time);
						relation.put("source", source);
						relation.put("sourceCid", scid);
						relation.put("targetCid", tcid);
						relation.put("target", target);
						relation.put("value", num);
						
						relations.add(relation);
					}
					for(int z = 0; z < l2; z++) {
						String[] info = ((String) map[j][i].get(z)).split(",");
						String source = info[0];
						String target = info[1];
						String time = info[2];
						int num = Integer.parseInt(info[3]);
						allNum += num;
						
						JSONObject relation = new JSONObject();
						relation.put("hour", time);
						relation.put("source", source);
						relation.put("sourceCid", tcid);
						relation.put("targetCid", scid);
						relation.put("target", target);
						relation.put("value", num);
						
						relations.add(relation);
					}
					
					JSONObject linkObj = new JSONObject();
					linkObj.put("source", scid);
					linkObj.put("target", tcid);
					linkObj.put("value", allNum);
					linkObj.put("relations", relations);
					links.add(linkObj);
				}
			}
		}
		
		for(int i = 0; i < count; i++) {
			JSONObject nodeObj = new JSONObject();
			nodeObj.put("id", (int)cidMap1.get(i));
			nodeObj.put("color", colorList[i]);
			nodes.add(nodeObj);
		}
		
		res.put("nodes", nodes);
		res.put("links", links);
		
		response.setContentType("application/json");
		response.setCharacterEncoding("utf-8");
		// Get the printwriter object from response to write the required json object to the output stream      
		PrintWriter out = response.getWriter();
		// Assuming your json object is **jsonObject**, perform the following, it will return your json object  
		out.print(res);
		out.flush();
	}

}
