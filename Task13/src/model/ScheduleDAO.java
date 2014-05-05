package model;
import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/*
 * Establish 3 tables
 * Before you change YOU NEED TO CHANGE the filePath of the data.txt
 * 
 * stops(
 * stop_id char(6),
 * stop_name char(50),
 * stop_lat char(11),
 * stop_lon char(11)
 * )
 * 
 * 
 * stops(
 * stop_id char(6),
 * stop_name char(50),
 * stop_lat char(11),
 * stop_lon char(11)
 * )
 * 
 * trips(
 * trip_id char(32),
 * route_id char(10),
 * direction_id int(1)
 * )
 * 
 */
public final class ScheduleDAO {

	private List<Connection> connectionPool = new ArrayList<Connection>();

	private String jdbcDriver;
	private String jdbcURL;
	private String tableName = "schedule";
	private String filePath = "D:/workspace/Task13/src/data/";		//change this to your data filePath
	
	
	public ScheduleDAO(String jdbcDriver, String jdbcURL) throws Exception {
		this.jdbcDriver = jdbcDriver;
		this.jdbcURL = jdbcURL;
		
		if (!(tableExists("stops") || tableExists("stop_times") || tableExists("trips"))) {
			initializeDB();
		} else {
			System.out.println("Data already initialized");
		}
	}
	
	private void createTable(String tableName) throws Exception {
		Connection con = null;
		try {
			con = getConnection();
			Statement stmt = con.createStatement();
			if (tableName.equals("stop_times")) {
				stmt.executeUpdate("CREATE TABLE "
						+ tableName
						+ " ("
						+ "trip_id char(32),"
						+ "departure_time char(8),"
						+ "stop_id char(6)"
						+ ")");
			} else if (tableName.equals("stops")) {
				stmt.executeUpdate("CREATE TABLE "
						+ tableName
						+ " ("
						+ "stop_id char(6),"
						+ "stop_name char(100),"
						+ "stop_lat char(11),"
						+ "stop_lon char(11)"
						+ ")");
			} else if (tableName.equals("trips")) {
				stmt.executeUpdate("CREATE TABLE "
						+ tableName
						+ " ("
						+ "trip_id char(32),"
						+ "route_id char(10),"
						+ "direction_id int(1)"
						+ ")");
			} else {		//no use currently
				stmt.executeUpdate("CREATE TABLE "
						+ this.tableName
						+ " ("
						+ "trip_id char(32),"
						+ "route_id char(10),"
						+ "direction_id int(1),"
						+ "departure_time char(8),"
						+ "stop_id char(6),"
						+ "stop_name char(100),"
						+ "stop_lat char(11),"
						+ "stop_lon char(11)"
						+ ")");
			}
			stmt.close();
			releaseConnection(con);
			
		} catch (SQLException e) {
			try {
				if (con != null)
					con.close();
			} catch (SQLException e2) { /* ignore */
			}
			throw new Exception(e);
		}
	}
	
	/* 
	 * #IMPORTANT# 
	 * ONLY use when the first time import data from txt 
	 * Read data from PAAC txt, import them into three tables
	 * Create a final schedule table for our own use
	 * 
	 */
	public void initializeDB() throws Exception {
		createTable("stop_times");
		createTable("stops");
		createTable("trips");
		
		Scanner timeScanner = null;
		Scanner stopScanner = null;
		Scanner tripScanner = null;
		
		try {	// import data
			timeScanner = new Scanner(new File(filePath + "stop_times.txt"));
			stopScanner = new Scanner(new File(filePath + "stops.txt"));
			tripScanner = new Scanner(new File(filePath + "trips.txt"));
			
			// skip the talbe title
			timeScanner.nextLine();
			stopScanner.nextLine();
			tripScanner.nextLine();
			
			int counter = 0;		// for test use
			int limit = 500;
			
			while (timeScanner.hasNextLine()) {
				String line = timeScanner.nextLine();
				String[] fields = line.split(",");
				
				Connection con = null;
				try {
					con = getConnection();
					PreparedStatement pstmt = con
							.prepareStatement("INSERT INTO "
									+ "stop_times"
									+ " (trip_id,departure_time,stop_id) VALUES (?,?,?)");

					pstmt.setString(1, fields[0]);
					pstmt.setString(2, fields[2]);
					pstmt.setString(3, fields[3]);

					System.out.println("Query: " + pstmt.toString());

					int count = pstmt.executeUpdate();
					if (count != 1)
						throw new SQLException("Insert updated " + count + " rows");
					pstmt.close();
					releaseConnection(con);
					
					if (counter++ > limit) {
						counter = 0;
						break;
					}

				} catch (Exception e) {
					try {
						if (con != null)
							con.close();
					} catch (SQLException e2) { // ignore
					}
					throw new Exception(e);
				}
			}
			
			while (stopScanner.hasNextLine()) {
				String line = stopScanner.nextLine();
				String[] fields = line.split(",");
				
				Connection con = null;
				try {
					con = getConnection();
					PreparedStatement pstmt = con
							.prepareStatement("INSERT INTO "
									+ "stops"
									+ " (stop_id,stop_name,stop_lat,stop_lon) VALUES (?,?,?,?)");

					pstmt.setString(1, fields[0]);
					pstmt.setString(2, fields[2]);
					pstmt.setString(3, fields[4]);
					pstmt.setString(4, fields[5]);

					System.out.println("Query: " + pstmt.toString());

					int count = pstmt.executeUpdate();
					if (count != 1)
						throw new SQLException("Insert updated " + count + " rows");
					pstmt.close();
					releaseConnection(con);
					
					if (counter++ > limit) {
						counter = 0;
						break;
					}

				} catch (Exception e) {
					try {
						if (con != null)
							con.close();
					} catch (SQLException e2) { // ignore
					}
					throw new Exception(e);
				}
			}
			
			while (tripScanner.hasNextLine()) {
				String line = tripScanner.nextLine();
				String[] fields = line.split(",");
				
				Connection con = null;
				try {
					con = getConnection();
					PreparedStatement pstmt = con
							.prepareStatement("INSERT INTO "
									+ "trips"
									+ " (trip_id,route_id,direction_id) VALUES (?,?,?)");

					pstmt.setString(1, fields[2]);
					pstmt.setString(2, fields[0]);
					pstmt.setString(3, fields[4]);

					System.out.println("Query: " + pstmt.toString());

					int count = pstmt.executeUpdate();
					if (count != 1)
						throw new SQLException("Insert updated " + count + " rows");
					pstmt.close();
					releaseConnection(con);
					
					if (counter++ > limit) {
						counter = 0;
						break;
					}

				} catch (Exception e) {
					try {
						if (con != null)
							con.close();
					} catch (SQLException e2) { // ignore
					}
					throw new Exception(e);
				}
			}
		} catch (FileNotFoundException e) {
			System.err.println(e.getMessage());
		} finally {
			if (timeScanner != null) {
				timeScanner.close();
			}
			if (stopScanner != null) {
				stopScanner.close();
			}
			if (tripScanner != null) {
				tripScanner.close();
			}
		}	// end import data
		
		Connection con = null;
		try {
			con = getConnection();
			PreparedStatement pstmt1 = con
					.prepareStatement("Create VIEW trip_time AS"
							+ "SELECT trips.trip_id,trips.route_id,trips.direction_id,"
							+ "stop_times.departure_time, stop_times.stop_id"
							+ "FROM trips INNER JOIN stop_times "
							+ "ON trips.trip_id=stop_times.trip_id;");

			System.out.println("Query: " + pstmt1.toString());

			int count1 = pstmt1.executeUpdate();
			if (count1 != 1)
				throw new SQLException("Insert updated " + count1 + " rows");
			pstmt1.close();
			releaseConnection(con);
			
			con = getConnection();
			PreparedStatement pstmt2 = con
					.prepareStatement("Create VIEW trip_time AS"
							+ "SELECT trips.trip_id,trips.route_id,trips.direction_id,"
							+ "stop_times.departure_time, stop_times.stop_id"
							+ "FROM trips INNER JOIN stop_times "
							+ "ON trips.trip_id=stop_times.trip_id;");

			System.out.println("Query: " + pstmt2.toString());

			int count2 = pstmt2.executeUpdate();
			if (count2 != 1)
				throw new SQLException("Insert updated " + count2 + " rows");
			pstmt2.close();
			releaseConnection(con);

		} catch (Exception e) {
			try {
				if (con != null)
					con.close();
			} catch (SQLException e2) { // ignore
			}
			throw new Exception(e);
		}
		
	}

	private synchronized Connection getConnection() throws Exception {
		if (connectionPool.size() > 0) {
			return connectionPool.remove(connectionPool.size() - 1);
		}

		try {
			Class.forName(jdbcDriver);
		} catch (ClassNotFoundException e) {
			throw new Exception(e);
		}

		try {
			return DriverManager.getConnection(jdbcURL);
		} catch (SQLException e) {
			throw new Exception(e);
		}
	}

	private synchronized void releaseConnection(Connection con) {
		connectionPool.add(con);
	}

	private boolean tableExists(String tableName) throws Exception {
		Connection con = null;
		try {
			con = getConnection();
			DatabaseMetaData metaData = con.getMetaData();
			ResultSet rs = metaData.getTables(null, null, tableName, null);

			boolean answer = rs.next();

			rs.close();
			releaseConnection(con);

			return answer;

		} catch (SQLException e) {
			try {
				if (con != null)
					con.close();
			} catch (SQLException e2) { /* ignore */
			}
			throw new Exception(e);
		}
	}

}
