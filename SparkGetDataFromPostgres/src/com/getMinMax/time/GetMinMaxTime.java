package com.getMinMax.time;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class GetMinMaxTime {
	String minTime;
public String getMinTime(String DataBaseName, String user, String pass, String sql) throws IOException
{
	
	try {
		Connection con = DriverManager.getConnection("jdbc:postgresql://ip:5432/" + DataBaseName, user,
				pass);
		System.out.println("\n<-----------Getting MinTime From Postgres ----------->\n");
		PreparedStatement pre = con.prepareStatement(sql);
		ResultSet r = pre.executeQuery();
		while (r.next()) {
			minTime = r.getBigDecimal("updated_on").toString();

		}
		FileOutputStream fos = new FileOutputStream("MinTime.txt");
		OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fos);
		BufferedWriter br = new BufferedWriter(outputStreamWriter);
		br.write(minTime);
		br.close();
		return minTime;

	} catch (SQLException e) {

		e.printStackTrace();
		return minTime;
	}
	
}

}
