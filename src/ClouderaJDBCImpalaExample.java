import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ClouderaJDBCImpalaExample {
	
	// here is an example query based on one of the Hue Beeswax sample tables 
	private static final String SQL_STATEMENT = "show tables";
	private static final String SQL_STATEMENT1 = "CREATE TABLE if not exists complex_struct_array (continent STRING, country STRUCT <name: STRING, city: ARRAY <STRING> >) STORED AS PARQUET";
	private static final String SQL_STATEMENT2 = "CREATE TABLE if not exists flat_struct_array (continent STRING, country STRING, city STRING)"; 
	private static final String SQL_STATEMENT3 = "INSERT INTO flat_struct_array VALUES ('North America', 'Canada', 'Toronto')";
	private static final String SQL_STATEMENT5 = " compute stats";
	private static final String SQL_STATEMENT4 = "Insert into complex_struct_array select * from flat_struct_array";
	
	// set the impalad host
	private static final String IMPALAD_HOST1 = "host-xxx";
	
	// port 21050 is the default impalad JDBC port 
	private static final String IMPALAD_JDBC_PORT = "21050";

	private static final String CONNECTION_URL = "jdbc:impala://" + IMPALAD_HOST1 + ':' + IMPALAD_JDBC_PORT + "/;" +
			"AuthMech=1;KrbRealm=CITIDSE.COM;KrbHostFQDN=xxx-xxx;KrbServiceName=impala;";

	private static final String JDBC_DRIVER_NAME = "com.cloudera.impala.jdbc4.Driver";

	public static void main(String[] args) {

		System.out.println("\n=============================================");
		System.out.println("Cloudera Impala JDBC Example");
		System.out.println("Using Connection URL: " + CONNECTION_URL);
		System.out.println("Running Query: " + SQL_STATEMENT);

		Connection con = null;

		
		 SimpleDateFormat time_formatter = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss.SSS");
			try {

			Class.forName(JDBC_DRIVER_NAME);
			System.out.println(CONNECTION_URL);

			con = DriverManager.getConnection(CONNECTION_URL);
			
			 String current_time_str = time_formatter.format(System.currentTimeMillis());
			 System.out.println(current_time_str);
			 Statement stmt = con.createStatement();
			 stmt.execute(SQL_STATEMENT1);
			 stmt.close();
			//con.close();
			current_time_str = time_formatter.format(System.currentTimeMillis());
			System.out.println(current_time_str);	Statement stmt1 = con.createStatement();
			stmt1.execute(SQL_STATEMENT2);
			stmt1.close();
			//	con.close();
			current_time_str = time_formatter.format(System.currentTimeMillis());
			System.out.println(current_time_str);	Statement stmt2 = con.createStatement();
			stmt2.execute(SQL_STATEMENT3);
			stmt2.close();
		//	con.close();
			current_time_str = time_formatter.format(System.currentTimeMillis());
			System.out.println(current_time_str);
			
			Statement stmt5 = con.createStatement();
			ResultSet rs5 = stmt5.executeQuery(SQL_STATEMENT);
			stmt5.close();
			System.out.println("\n== Begin Query Results ======================");
			
			current_time_str = time_formatter.format(System.currentTimeMillis());
			System.out.println(current_time_str);
			
			Statement stmt4 = con.createStatement();
			ResultSet rs4 = stmt4.executeQuery(SQL_STATEMENT);
			stmt4.close();

		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				con.close();
			} catch (Exception e) {
				// swallow
			}
		}
	}
}