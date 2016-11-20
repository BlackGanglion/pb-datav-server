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
 * Servlet implementation class createTable
 */
@WebServlet("/createTable")
public class createTable extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public createTable() {
        super();
        // TODO Auto-generated constructor stub
    }
    
    private void createFn(Connection con, String m, String d) {
    	String day = "2014-" + m + "-" + d;
		String table = "B_LEASEINFOHIS_SUM_2014_" + m + "_" + d;
		
		try {
			String createTableSQL = "CREATE TABLE " + table + 
					"(LEASEDATE VARCHAR2(10 BYTE), " +
					"LEASETIME VARCHAR2(2 BYTE), " + 
					"LEASESTATION VARCHAR2(64 BYTE), " +
					"RETURNSTATION VARCHAR2(64 BYTE), " + 
					"BIKENUM NUMBER)";
			
			Statement createTableStatement = con.createStatement();
			createTableStatement.execute(createTableSQL);
			createTableStatement.close();
			
			System.out.println(table + " create done");
			
			// 创建索引
			String LEASETIMEIndexName = "LT2014" + m + d;
			String LEASETIMEIndexSQL = "create index "
					+ LEASETIMEIndexName + " on "+ table + "(LEASETIME)";
			
			// System.out.println(LEASETIMEIndexSQL);
			Statement LEASETIMEIndexStatement = con.createStatement();
			LEASETIMEIndexStatement.execute(LEASETIMEIndexSQL);
			LEASETIMEIndexStatement.close();
			
			System.out.println(table + " LEASETIMEIndex done");
			
			String LEASESTATIONIndexName = "LS2014" + m + d;
			String LEASESTATIONIndexSQL = "create index "
					+ LEASESTATIONIndexName + " on " + table + "(LEASESTATION)";
			Statement LEASESTATIONIndeStatement = con.createStatement();
			LEASESTATIONIndeStatement.execute(LEASESTATIONIndexSQL);
			LEASESTATIONIndeStatement.close();
			
			System.out.println(table + " LEASESTATIONIndex done");
			
			String querySQL = "SELECT * FROM B_LEASEINFOHIS_SUM WHERE LEASEDATE = '" + day + "'";
			String insertSQL = "insert into "
					+ table + " ("
					+ querySQL + ")";
			
			Statement insertStatement = con.createStatement();
			insertStatement.executeUpdate(insertSQL);
			insertStatement.close();
			
			/*
			Statement queryStatement = con.createStatement();
			ResultSet queryRes = queryStatement.executeQuery(querySQL);
			
			queryRes.setFetchSize(5000);
			
			int count = 0;
			
			while(queryRes.next()) {
				if (count % 10000 == 0) {
					System.out.println(table + " row " + count);
				}
				count++;
				String LEASEDATE = queryRes.getString(1);
				String LEASETIME = queryRes.getString(2);
				String LEASESTATION = queryRes.getString(3);
				String RETURNSTATION = queryRes.getString(4);
				int BIKENUM = queryRes.getInt(5);
				
				String insertSQL = "INSERT INTO " +
				     table + 
				     "(LEASEDATE, LEASETIME, LEASESTATION, RETURNSTATION, BIKENUM) values('" +
				     LEASEDATE + "', '" + LEASETIME + "', '" + LEASESTATION + "', '" + RETURNSTATION + "', "+ BIKENUM + ")";
				
				Statement insertStatement = con.createStatement();
				insertStatement.executeUpdate(insertSQL);
				insertStatement.close();
			}
			
			queryStatement.close();
			*/
			
			System.out.println(table + " dataCopy done");
			
			// table 完成
			System.out.println(table + " all done");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
		}
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		Connection con = connect.dbConn();
		
		try {
			if (con == null) {
				System.out.println("connect failed");
	        }
			System.out.println("connect success");
			
			int[] dayList = { 31, 30, 31, 23 };
			for(int i = 3, j = 0; i <= 6; i++, j++) {
				int count = dayList[j];
				for(int z = 1; z <= count; z++) {
					this.createFn(con, String.format("%02d", i), String.format("%02d", z));
					//System.out.print("drop table B_LEASEINFOHIS_SUM_2014_" 
					// + String.format("%02d", i) + "_" + String.format("%02d", z) + ";");
				}
			}
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
		
		JSONObject res = new JSONObject();
		res.put("success", "create success");
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
