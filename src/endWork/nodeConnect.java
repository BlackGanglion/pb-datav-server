package endWork;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;

import org.json.simple.*;

import endWork.connect;

/**
 * Servlet implementation class nodeConnect
 */
@WebServlet("/nodeConnect.json")
public class nodeConnect extends HttpServlet {
    /**
     * @see HttpServlet#HttpServlet()
     */
	private static final String HOUR_TABLE = "B_LEASEINFOHIS_SUM_PART";
	
    public nodeConnect() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		Connection con = connect.dbConn();
		// TODO Auto-generated method stub
	
		// 三个参数，nodeId, day, hour(可选)
		String NodeIdList = request.getParameter("nodeId");
		String Day = request.getParameter("day");
		String Hour = request.getParameter("hour");
		
		String[] NodeIdArr = NodeIdList.split(",");
		
		JSONObject res = new JSONObject();
		
		String DB_TABLE = this.HOUR_TABLE;
		
		// nodeId循环，建立set集合
		// 节点信息集合
		HashSet<String> nodeHashSet = new HashSet<String>();
		
		for (int i = 0; i < NodeIdArr.length; i++) {
			nodeHashSet.add(NodeIdArr[i]);
		}
		
		JSONArray nodes = new JSONArray();
		JSONArray links = new JSONArray();
		
		try {
			if (con == null) {
				System.out.print("connect failed\n");
	        }
			System.out.print("connect success\n");
			
			/*
			 SELECT * FROM B_LEASEINFOHIS_SUM_PART partition(D20140416) WHERE LEASETIME = '08' 
			 AND ((LEASESTATION = '3214' AND RETURNSTATION = '2409') 
			 OR (LEASESTATION = '2409' AND RETURNSTATION = '3214'));
			 */
			
			// SQL预处理
			String sqlNode = "SELECT * FROM B_STATIONINFO_BRIEF WHERE STATIONID = ?";
					
			String sqlHour = "SELECT * FROM B_LEASEINFOHIS_SUM_PART partition(D" + Day + ")"
					+ " WHERE LEASETIME = ?"
					+ " AND ((LEASESTATION = ? AND RETURNSTATION = ?)"
					+ " OR (LEASESTATION = ? AND RETURNSTATION = ?))";
			
			String sqlNoHour = "SELECT * FROM B_LEASEINFOHIS_SUM_PART partition(D" + Day + ")"
					+ " WHERE ((LEASESTATION = ? AND RETURNSTATION = ?)"
					+ " OR (LEASESTATION = ? AND RETURNSTATION = ?))";
					
			PreparedStatement psNode = con.prepareStatement(sqlNode);
			PreparedStatement psHour = con.prepareStatement(sqlHour);
			PreparedStatement psNoHour = con.prepareStatement(sqlNoHour);
			
			Long t1 = System.currentTimeMillis();
			if(Hour != null) {
				for (int i = 0; i < NodeIdArr.length; i++) {
					psNode.setString(1, NodeIdArr[i]);
					ResultSet nodeResult = psNode.executeQuery();
					
					while(nodeResult.next()){
						JSONObject nodeObj = new JSONObject();
						nodeObj.put("id", nodeResult.getString(1));
						nodeObj.put("name", nodeResult.getString(2));
						nodeObj.put("x", nodeResult.getString(3));
						nodeObj.put("y", nodeResult.getString(4));
						nodeObj.put("bx", nodeResult.getString(5));
						nodeObj.put("by", nodeResult.getString(6));
						nodeObj.put("flag", nodeResult.getString(7));
						nodeObj.put("address", nodeResult.getString(8));
						nodeObj.put("servicetime", nodeResult.getString(9));
						nodes.add(nodeObj);
					}
					
					for (int j = i + 1; j < NodeIdArr.length; j++) {
						psHour.setString(1, Hour);
						psHour.setString(2, NodeIdArr[i]);
						psHour.setString(3, NodeIdArr[j]);
						psHour.setString(4, NodeIdArr[j]);
						psHour.setString(5, NodeIdArr[i]);
						
						ResultSet linkResult = psHour.executeQuery();
						
						linkResult.setFetchSize(10000);
						
						int bikeCount = 0;
						JSONArray relations = new JSONArray();
						while(linkResult.next()){
							String hour = linkResult.getString(2);
							String source = linkResult.getString(3);
							String target = linkResult.getString(4);
							int bikeNum = linkResult.getInt(5);
							
							JSONObject relation = new JSONObject();
							
							relation.put("hour", hour);
							relation.put("source", source);
							relation.put("target", target);
							relation.put("value", bikeNum);
							
							relations.add(relation);
							
							bikeCount += bikeNum;
						}
						
						if (bikeCount != 0) {
							JSONObject linkObj = new JSONObject();
							linkObj.put("source", NodeIdArr[i]);
							linkObj.put("target", NodeIdArr[j]);
							linkObj.put("value", bikeCount);
							linkObj.put("relations", relations);
							links.add(linkObj);
						}
					}
				}
			} else {
				for (int i = 0; i < NodeIdArr.length; i++) {
					psNode.setString(1, NodeIdArr[i]);
					ResultSet nodeResult = psNode.executeQuery();
					
					while(nodeResult.next()){
						JSONObject nodeObj = new JSONObject();
						nodeObj.put("id", nodeResult.getString(1));
						nodeObj.put("name", nodeResult.getString(2));
						nodeObj.put("x", nodeResult.getString(3));
						nodeObj.put("y", nodeResult.getString(4));
						nodeObj.put("bx", nodeResult.getString(5));
						nodeObj.put("by", nodeResult.getString(6));
						nodeObj.put("flag", nodeResult.getString(7));
						nodeObj.put("address", nodeResult.getString(8));
						nodeObj.put("servicetime", nodeResult.getString(9));
						nodes.add(nodeObj);
					}
					
					for (int j = i + 1; j < NodeIdArr.length; j++) {
						psNoHour.setString(1, NodeIdArr[i]);
						psNoHour.setString(2, NodeIdArr[j]);
						psNoHour.setString(3, NodeIdArr[j]);
						psNoHour.setString(4, NodeIdArr[i]);
						
						ResultSet linkNoResult = psNoHour.executeQuery();
						
						linkNoResult.setFetchSize(10000);
						
						int bikeCount = 0;
						JSONArray relations = new JSONArray();
						while(linkNoResult.next()){
							String hour = linkNoResult.getString(2);
							String source = linkNoResult.getString(3);
							String target = linkNoResult.getString(4);
							int bikeNum = linkNoResult.getInt(5);
							
							JSONObject relation = new JSONObject();
							
							relation.put("hour", hour);
							relation.put("source", source);
							relation.put("target", target);
							relation.put("value", bikeNum);
							
							relations.add(relation);
							
							bikeCount += bikeNum;
						}
						
						if (bikeCount != 0) {
							JSONObject linkObj = new JSONObject();
							linkObj.put("source", NodeIdArr[i]);
							linkObj.put("target", NodeIdArr[j]);
							linkObj.put("value", bikeCount);
							linkObj.put("relations", relations);
							links.add(linkObj);
						}
					}
				}
			}
			
			Long t2 = System.currentTimeMillis();
	    	System.out.print(t2 - t1 + "\n");
			
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
		
		res.put("nodes", nodes);
		res.put("links", links);
		
		response.setContentType("application/json");
		response.setCharacterEncoding("utf-8");
		// Get the printwriter object from response to write the required json object to the output stream      
		PrintWriter out = response.getWriter();
		// Assuming your json object is **jsonObject**, perform the following, it will return your json object  
		out.print(res);
		out.flush();
		
		/*
		Map<String, String[]> query = request.getParameterMap();
		
		HashMap<String, String> nodeHashMap = new HashMap<String, String>();
		
		JSONObject res = new JSONObject();
		
		JSONArray nodes = new JSONArray();
		for (Map.Entry<String, String[]> item : query.entrySet()) {
			JSONObject obj = new JSONObject();
			String id = item.getKey();
			obj.put("id", id);
			String[] names = item.getValue();
			obj.put("name", names[0]);
			nodes.add(obj);
			// 放入HashMap
			nodeHashMap.put(id, names[0]);
		}
		
		res.put("nodes", nodes);
		
		Connection con = connect.dbConn();
		
		try {
			if (con == null) {
				System.out.print("connect failed");
	        }
			System.out.print("connect success");
			Statement sql = con.createStatement();
			
			ResultSet result = sql.executeQuery("SELECT * FROM B_LEASEINFOHIS_SUM_BYDAY WHERE ROWNUM <= 200000");
			
			JSONArray links = new JSONArray();
			while (result.next()) {
				String source = result.getString(2);
				String target = result.getString(3);
				
				// System.out.println(source + " " + target + "" + nodeHashMap.get(source) + "" + nodeHashMap.get(target));
				
				if (nodeHashMap.get(source) != null && nodeHashMap.get(target) != null) {
					JSONObject obj = new JSONObject();
					obj.put("source", source);
					obj.put("target", target);
					obj.put("value", result.getInt(4));
					links.add(obj);
				}
	        }
			
			res.put("links", links);
			
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
		
		response.setContentType("application/json");
		response.setCharacterEncoding("utf-8");
		// Get the printwriter object from response to write the required json object to the output stream      
		PrintWriter out = response.getWriter();
		// Assuming your json object is **jsonObject**, perform the following, it will return your json object  
		out.print(res);
		out.flush();
		*/
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}
