package spatk.com.controller.getdata;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class getlastRowTime {
	String LastTime;

	public String getCall(String DataBaseName, String user, String pass, String sql) throws ClassNotFoundException {
		try {
			Connection con = DriverManager.getConnection("jdbc:postgresql://IP:5432/" + DataBaseName, user,
					pass);
			System.out.println("\n<-----------Getting MaxTime From Postgres ----------->\n");
			PreparedStatement pre = con.prepareStatement(sql);
			ResultSet r = pre.executeQuery();
			while (r.next()) {
				LastTime = r.getBigDecimal("updated_on").toString();

			}
			return LastTime;

		} catch (SQLException e) {

			//e.printStackTrace();
			return LastTime;
		}

	}
}
