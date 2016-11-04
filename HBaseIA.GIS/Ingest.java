package HBaseIA.GIS;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.collections.iterators.ArrayIterator;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import ch.hsr.geohash.GeoHash;

import com.google.common.base.Splitter;

/**
 * 读取文本数据里面的每一行，lat，lon用geohash编码后作为行键，其余属性作为列簇里面的每一列，全部存储到hbase
 * @author 大象
 *
 */
public class Ingest {

	private static final byte[] FAMILY = "a".getBytes();
	private static final String[] COLUMNS = new String[] { "lon", "lat", "id",
			"name", "address", "city", "url", "phone", "type", "zip" };
	private static final ArrayIterator COLS = new ArrayIterator(COLUMNS);
	private static final Splitter SPLITTER = Splitter.on('\t').trimResults()
			.limit(COLUMNS.length);

	public static void main(String[] args) throws IOException {

		if (args.length != 2) {
			System.out.println("输入参数不准确");
			System.exit(0);
		}

		HTable table = new HTable(args[0]);
		table.setAutoFlush(false);

		BufferedReader reader = new BufferedReader(new FileReader(args[1]));
		String line = reader.readLine();
		int records = 0;
		long start = System.currentTimeMillis();

		while ((line = reader.readLine()) != null) {
			COLS.reset();
			Iterator<String> vals = SPLITTER.split(line).iterator();
			Map<String, String> row = new HashMap<String, String>(
					COLUMNS.length);

			while (vals.hasNext() && COLS.hasNext()) {
				String col = (String) COLS.next();
				String val = vals.next();
				row.put(col, val);
			}

			double lat = Double.parseDouble(row.get("lat"));
			double lon = Double.parseDouble(row.get("lon"));
			String rowkey = GeoHash.withCharacterPrecision(lat, lon, 12)
					.toBase32();
			Put put = new Put(rowkey.getBytes());
			for (Map.Entry<String, String> e : row.entrySet()) {
				put.add(FAMILY, e.getKey().getBytes(), e.getValue().getBytes());
			}

			table.put(put);
			records++;
		}
		table.flushCommits();
		long end = System.currentTimeMillis();
		System.out.println(String.format("Geohash编码%s条数据，用了%sms",
				records, end - start));

		reader.close();
		table.close();
	}
}
