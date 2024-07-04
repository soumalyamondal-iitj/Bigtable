package org.iitj.project;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;

/*
* Use Google Bigtable to store and analyze sensor data.
*/
public class BigTable {
// TODO: Fill in information for your database
	public final String projectId = "my-project-g23ai1042";
	public final String instanceId = "iitjdb";
	public final String COLUMN_FAMILY = "sensor";
	public final String tableId = "weather"; // TODO: Must change table name if sharing my database
	public BigtableDataClient dataClient;
	public BigtableTableAdminClient adminClient;

	public static void main(String[] args) throws Exception {
		BigTable testbt = new BigTable();
		testbt.run();
	}

	public void connect() throws IOException {
		// TODO: Write code to create a data client and admin client to connect to
		// Google Bigtable
		// See sample code in HelloWorld.java for help
		// Creates the settings to configure a bigtable data client.
		BigtableDataSettings settings = BigtableDataSettings.newBuilder().setProjectId(projectId)
				.setInstanceId(instanceId).build();

		// Creates a bigtable data client.
		dataClient = BigtableDataClient.create(settings);

		// Creates the settings to configure a bigtable table admin client.
		BigtableTableAdminSettings adminSettings = BigtableTableAdminSettings.newBuilder().setProjectId(projectId)
				.setInstanceId(instanceId).build();

		// Creates a bigtable table admin client.
		adminClient = BigtableTableAdminClient.create(adminSettings);
		System.out.println("1. Connected to Google BigTable projectId="+projectId+", and instanceId="+instanceId);
	}

	public void run() throws Exception {
		
		try {
			connect();
			
			deleteTable();
			createTable();
			loadData();
		
			int temp = query1();
			System.out.println("4. Temperature at Vancouver on 2022-10-01 at 10 a.m.: " + temp);
			int windspeed = query2();
			System.out.println("5. Highest wind speed in the month of September 2022 in Portland: " + windspeed);
	
			ArrayList<Object[]> data = query3();
			StringBuffer buf = new StringBuffer();
			for (int i = 0; i < data.size(); i++) {
				Object[] vals = data.get(i);
				for (int j = 0; j < vals.length; j++)
					buf.append(vals[j] + " ");
				buf.append("\n");
			}
			System.out.println("6. All the readings for SeaTac for October 2, 2022:\n" +buf.toString());
			
			temp = query4();
			System.out.println("7. Highest temperature at any station in the summer months of 2022 (July (7), August (8)): " + temp);
		}catch(Exception e) {
			e.printStackTrace();
			throw e;			
		}
		finally {
			close();
		}		
	}

	/**
	 * Close data and admin clients
	 */
	public void close() {
		dataClient.close();
		adminClient.close();
		System.out.println("\n8. Closed data and admin clients.");
	}

	public void createTable() {
		// TODO: Create a table to store sensor data.
		// Checks if table exists, creates table if does not exist.
		if (!adminClient.exists(tableId)) {
			System.out.println("2. Creating table: " + tableId);
			CreateTableRequest createTableRequest = CreateTableRequest.of(tableId).addFamily(COLUMN_FAMILY);
			adminClient.createTable(createTableRequest);
			System.out.printf("Table %s created successfully%n", tableId);
		}
	}

	/**
	 * Loads data into database. Data is in CSV files. Note that must convert to
	 * hourly data. Take the first reading in a hour and ignore any others.
	 */
	public void loadData() throws Exception {
		String path = "C:\\IITJ\\BDM\\Data\\";
		// TODO: Load data from CSV files into sensor table
		// Note: There are multiple different ways that you can decide on how to
		// organize this data into columns
		try {
			System.out.println("3. Loading data ...");
			
			// SeaTac station id is SEA
			System.out.println("Loading data for SeaTac");
			loadCsvData(path + "SeaTac.csv", "SEA");

			// Vancouver station id is YVR
			System.out.println("Loading data for Vancouver");
			loadCsvData(path + "Vancouver.csv", "YVR");

			// Portland station id is PDX
			System.out.println("Loading data for Portland");
			loadCsvData(path + "Portland.csv", "PDX");
			
			System.out.println("Data loading successful.");
			
		} catch (Exception e) {
			throw new Exception(e);
		}
	}
	

	private void loadCsvData(String filePath, String stationId) throws IOException {
		try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
			String line;
			int lineCount = 0;
			BulkMutation bulkMutation = BulkMutation.create(tableId);
			while ((line = br.readLine()) != null) {
				++lineCount;
				if (lineCount <= 2)
					continue;

				String[] values = line.split(",");
				String date = values[1];
				String time = values[2];
				String rowKey = stationId + "#" + date + "#" + time;
				Mutation mutation = Mutation.create()
						.setCell(COLUMN_FAMILY, "pseudoJulianDate", values[0])
						.setCell(COLUMN_FAMILY, "temperature", values[3])
						.setCell(COLUMN_FAMILY, "dewpoint", values[4])
						.setCell(COLUMN_FAMILY, "relhum", values[5])
						.setCell(COLUMN_FAMILY, "speed", values[6])
						.setCell(COLUMN_FAMILY, "gust", values[7])
						.setCell(COLUMN_FAMILY, "pressure", values[8]);
				bulkMutation.add(rowKey, mutation);
			}
			dataClient.bulkMutateRows(bulkMutation);
		}
	}

	/**
	 * Query returns the temperature at Vancouver on 2022-10-01 at 10 a.m.
	 **
	 * @return ResultSet
	 * @throws SQLException if an error occurs
	 */
	public int query1() throws Exception {
		Query query = Query.create(tableId).prefix("YVR#2022-10-01#10");
		ServerStream<Row> rows = dataClient.readRows(query);
		for (Row row : rows) {
			List<RowCell> cells = row.getCells(COLUMN_FAMILY, "temperature");
			if (!cells.isEmpty()) {
				return Integer.parseInt(cells.get(0).getValue().toStringUtf8());
			}
		}
		return 0;
	}

	/**
	 * Query returns the highest wind speed in the month of September 2022 in
	 * Portland.
	 **
	 * @return ResultSet
	 * @throws SQLException if an error occurs
	 */
	public int query2() throws Exception {
		Query query = Query.create(tableId).prefix("PDX#2022-09");
		ServerStream<Row> rows = dataClient.readRows(query);
		int maxWindSpeed = 0;
		for (Row row : rows) {
			List<RowCell> cells = row.getCells(COLUMN_FAMILY, "speed");
			if (!cells.isEmpty()) {
				int windspeed = Integer.parseInt(cells.get(0).getValue().toStringUtf8());
				if (windspeed > maxWindSpeed) {
					maxWindSpeed = windspeed;
				}
			}
		}
		return maxWindSpeed;
	}

	/**
	 * Query returns all the readings for SeaTac for October 2, 2022. Return as an
	 * ArrayList of objects arrays. Each object array should have fields: date
	 * (string), hour (string), temperature (int), dewpoint (int), humidity
	 * (string), windspeed (string), pressure (string)
	 **
	 * @return ResultSet
	 * @throws SQLException if an error occurs
	 */
	
	public ArrayList<Object[]> query3() throws Exception {
		Query query = Query.create(tableId).prefix("SEA#2022-10-02");
		ServerStream<Row> rows = dataClient.readRows(query);
		ArrayList<Object[]> data = new ArrayList<>();
		//Add columns
		data.add(addColumnsHeaders());
		
		for (Row row : rows) {
			//List<RowCell> cells = row.getCells(COLUMN_FAMILY);
			Object[] rowData = new Object[9];
			
			List<RowCell> cells1 = row.getCells(COLUMN_FAMILY, "pseudoJulianDate");
			rowData[0] = cells1.get(0).getValue().toStringUtf8();
			
			rowData[1] = row.getKey().toStringUtf8().split("#")[1];
			rowData[2] = row.getKey().toStringUtf8().split("#")[2];
			
			List<RowCell> cells3 = row.getCells(COLUMN_FAMILY, "temperature");
			rowData[3] = cells3.get(0).getValue().toStringUtf8();
			
			List<RowCell> cells4 = row.getCells(COLUMN_FAMILY, "dewpoint");			
			rowData[4] = cells4.get(0).getValue().toStringUtf8();	
			
			List<RowCell> cells5 = row.getCells(COLUMN_FAMILY, "relhum");			
			rowData[5] = cells5.get(0).getValue().toStringUtf8();	
			
			List<RowCell> cells6 = row.getCells(COLUMN_FAMILY, "speed");			
			rowData[6] = cells6.get(0).getValue().toStringUtf8();
			
			List<RowCell> cells7 = row.getCells(COLUMN_FAMILY, "gust");			
			rowData[7] = cells7.get(0).getValue().toStringUtf8();
			
			List<RowCell> cells8 = row.getCells(COLUMN_FAMILY, "pressure");			
			rowData[8] = cells8.get(0).getValue().toStringUtf8();
			
			data.add(rowData);
		}
		return data;
	}
	private Object[] addColumnsHeaders() {
		// TODO Auto-generated method stub
		Object[] rowData = new Object[9];
		rowData[0] = "Pseudo-Julian-Date";
		rowData[1] = "Date";
		rowData[2] = "Time";
		rowData[3] = "Temperature";
		rowData[4] = "Dewpoint";
		rowData[5] = "Relhum";
		rowData[6] = "Speed";
		rowData[7] = "Gust";
		rowData[8] = "Pressure";
		return rowData;
	}


	
	/**
	 * Query returns the highest temperature at any station in the summer months of
	 * 2022 (July (7), August (8)).
	 **
	 * @return ResultSet
	 * @throws SQLException if an error occurs
	 */
	public int query4() throws Exception {
		Query query = Query.create(tableId).range("SEA#2022-07", "SEA#2022-09");
		ServerStream<Row> rows = dataClient.readRows(query);
		int maxTemp = -100;
		for (Row row : rows) {
			List<RowCell> cells = row.getCells(COLUMN_FAMILY, "temperature");
			if (!cells.isEmpty()) {
				int temp = Integer.parseInt(cells.get(0).getValue().toStringUtf8());
				if (temp > maxTemp) {
					maxTemp = temp;
				}
			}
		}
		return maxTemp;
	}


	/**
	 * Delete the table from Bigtable.
	 */
	public void deleteTable() {
		System.out.println("Deleting table: " + tableId);
		try {
			adminClient.deleteTable(tableId);
			System.out.printf("Table %s deleted successfully%n", tableId);
		} catch (NotFoundException e) {
			System.err.println("Failed to delete a non-existent table: " + e.getMessage());
		}
	}
}