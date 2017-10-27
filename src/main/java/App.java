import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class App {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Class.forName("com.impetus.fabric.jdbc.FabricDriver");
		Connection conn = DriverManager.getConnection("jdbc:fabric:///home/impadmin/Blockchain_Java/blockchain-query");
		Statement stat = conn.createStatement();
		ResultSet rs = stat.executeQuery("select * from block");
		System.out.println("block_num -- transaction_id -- channel_id -- transaction_data");
		while(rs.next()) {
			System.out.println(rs.getInt(1) + "--" + rs.getString("transaction_id") + "--" + rs.getString("channel_id") + "--" + rs.getString("transaction_data"));
		}

	}

}
