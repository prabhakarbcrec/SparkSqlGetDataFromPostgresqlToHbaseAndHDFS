package spatk.com.controller.getdata;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;



public class PutHbase implements Serializable {

	/**
	 * 
	 */

	private static final long serialVersionUID = 1L;

	@SuppressWarnings("deprecation")
	public void getItToHbase(allDatawillComeHereAsAnArgument, Configuration connect) throws IOException {
		
	//Making key here//
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		HTable table = new HTable(connect, "tablename");                 
		Put p = new Put(Bytes.toBytes(key));
		p.add(Bytes.toBytes("Static Data"), Bytes.toBytes("vesselname"), Bytes.toBytes(vessel_name));
		p.add(Bytes.toBytes("Static Data"), Bytes.toBytes("mmsi"), Bytes.toBytes(mmsi_no));
		p.add(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("latitude"), Bytes.toBytes(latitude));
		p.add(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("longitude"), Bytes.toBytes(longitude));
		p.add(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("course"), Bytes.toBytes(course));
		p.add(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("speed"), Bytes.toBytes(speed));

		table.put(p);
		table.close();
		System.out.println("\ninserted into hbase....Table");
	}

}
