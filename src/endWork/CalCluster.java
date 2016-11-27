package endWork;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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
		
		int count = clustersInfo.getCount();
		List<Cluster> nodeMap = clustersInfo.getNodeMap();
		
		int map[][] = new int[count + 10][count + 10];
		
		// 建图
		for (int i = 0; i < count; i++) {
			for (int j = 0; j < count; j++) {
				map[i][j] = 0;
			}
		}
		
		JSONObject res = new JSONObject();
		
		// 建立映射关系
		Map nodeIdMap = new HashMap();
		JSONArray nodes = new JSONArray();
		for(int i = 0; i < nodeMap.size(); i++)  {  
			Cluster item = nodeMap.get(i);
			int id = item.getId();
			int cid = item.getClusterId();
			nodeIdMap.put(id, cid);
			JSONObject nodeObj = new JSONObject();
			nodeObj.put("id", id);
			nodes.add(nodeObj);
        }
		
		Connection con = connect.dbConn();
		
		try {
			if (con == null) {
				System.out.print("connect failed");
	        }
			System.out.print("connect success");
			
			Long t1 = System.currentTimeMillis();
			
			Statement sql = con.createStatement();
			ResultSet result = sql.executeQuery("SELECT * FROM B_LEASEINFOHIS_SUM");
			
			result.setFetchSize(100000);
			int i = 1;
			while (result.next()) {
				String sStr = result.getString(3);
				String tStr = result.getString(4);
				if (isInt(sStr) && isInt(tStr)) {
					int source = Integer.parseInt(sStr);
					int target = Integer.parseInt(tStr);
					
					// System.out.println(source);
					// System.out.println(target);
					
					int bikeNum = result.getInt(5);
					Object sourceCid = nodeIdMap.get(source);
					Object targetCid = nodeIdMap.get(target);
					
					if (sourceCid != null && targetCid != null) {
						map[(int) sourceCid][(int) targetCid] = map[(int) sourceCid][(int) targetCid] + bikeNum;
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
				if (map[i][j] != 0 && map[j][i] != 0) {
					JSONObject linkObj = new JSONObject();
					linkObj.put("source", i);
					linkObj.put("target",j);
					linkObj.put("value", map[i][j] + map[j][i]);
					links.add(linkObj);
				}
			}
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
