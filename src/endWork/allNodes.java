package endWork;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;

import org.json.simple.*;
import endWork.connect;

/**
 * Servlet implementation class allNodes
 */
@WebServlet("/allNodes.json")
public class allNodes extends HttpServlet {
    /**
     * @see HttpServlet#HttpServlet()
     */
    public allNodes() {
        super();
        // TODO Auto-generated constructor stub
        
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		Connection con = connect.dbConn();
		
		// TODO Auto-generated method stub
		JSONObject res = new JSONObject();
		
		try {
			if (con == null) {
				System.out.print("connect failed");
	        }
			System.out.print("connect success");
			Statement sql = con.createStatement();
			ResultSet result = sql.executeQuery("SELECT * FROM B_STATIONINFO_BRIEF");
			
			result.setFetchSize(3000);
			
			JSONArray nodes = new JSONArray();
			while (result.next()) {
				// 去除三个干扰点
				int id = result.getInt(1);
				if (id == 2018 || id == 2221 || id == 3629) continue;
				JSONObject obj = new JSONObject();
				obj.put("id", id);
				obj.put("index", id);
				obj.put("name", result.getString(2));
				obj.put("x", result.getDouble(5));
				obj.put("y", result.getDouble(6));
				obj.put("rx", result.getDouble(3));
				obj.put("ry", result.getDouble(4));
				obj.put("flag", result.getString(7));
				obj.put("address", result.getString(8));
				obj.put("servicetime", result.getString(9));
				nodes.add(obj);
	        }
			
			res.put("nodes", nodes);
			
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
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}
