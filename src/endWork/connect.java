package endWork;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class connect {
	private static final long serialVersionUID = 1L;
	
	public static final String DBDRIVER = "oracle.jdbc.driver.OracleDriver";
	// 定义数据库的连接地址
	// public static final String DBURL = "jdbc:oracle:thin:@localhost:49161/xe";
	 public static final String DBURL = "jdbc:oracle:thin:@10.211.55.11:1521:ora11g";
	//端口号后标识符可以通过在doc下运行lsnrctl status查看  default:1521
	// 数据库的连接用户名
	public static final String DBUSER = "system";
	// 数据库的连接密码
	public static final String DBPASS = "oracle";
	
	public static Connection dbConn() {
        Connection conn = null;
        try {
        	Class.forName(DBDRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
        	Long t1 = System.currentTimeMillis();
        	conn = DriverManager.getConnection(DBURL, DBUSER, DBPASS);
        	Long t2 = System.currentTimeMillis();
        	System.out.print(t2 - t1);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
     }
}
