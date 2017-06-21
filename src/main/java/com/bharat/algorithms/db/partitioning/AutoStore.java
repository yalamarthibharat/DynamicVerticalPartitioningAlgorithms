package com.bharat.algorithms.db.partitioning;

import com.bharat.dbconnections.SQLServerConnectionFactory;
import com.bharat.propertyhelper.KeyConstants;
import com.bharat.propertyhelper.PropertyHelper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Scanner;

/*
 * This is the program for autostore ---> for TPC-H lineitem table 
 */
public class AutoStore {
	private static final int maxQueryCost = 99999;
	public static int minQueryCost = 525;
	public static int whichal = 0;
	public static String attributes = PropertyHelper.getValueFromConfig(KeyConstants.ATTRIBUTES);
	public static String attributes1 = PropertyHelper.getValueFromConfig(KeyConstants.ATTRIBUTES1);
	public static boolean left = false, first = true, right = false, initial = true;
	public static final int windowSize = 6;
	static String timeStamp;
	static String timeStamp1;

	public static int getWhichal() {
		return whichal;
	}

	public static void setWhichal(int whichal) {
		AutoStore.whichal = whichal;
	}

	static String[] a = new String[windowSize];
	static int windowIndex = 0;
	static String table = PropertyHelper.getValueFromConfig(KeyConstants.TABLE);
	static String[] oldScheme = new String[20];
	static int[][] affinityMatrix = new int[17][17];
	static String[] attrOrder = {"[L_ORDERKEY]", "[L_PARTKEY]", "[L_SUPPKEY]", "[L_LINENUMBER]", "[L_QUANTITY]", "[L_EXTENDEDPRICE]", "[L_DISCOUNT]", "[L_TAX]", "[L_RETURNFLAG]", "[L_LINESTATUS]", "[L_SHIPDATE]", "[L_COMMITDATE]", "[L_RECEIPTDATE]", "[L_SHIPINSTRUCT]", "[L_SHIPMODE]", "[L_COMMENT]", "[SKIP]"}; // attribute ordering
	static int[] bestSplitVector = new int[16];
	static int[] previousSplit = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
	static String[] previousAttrOrder = {"[L_ORDERKEY]", "[L_PARTKEY]", "[L_SUPPKEY]", "[L_LINENUMBER]", "[L_QUANTITY]", "[L_EXTENDEDPRICE]", "[L_DISCOUNT]", "[L_TAX]", "[L_RETURNFLAG]", "[L_LINESTATUS]", "[L_SHIPDATE]", "[L_COMMITDATE]", "[L_RECEIPTDATE]", "[L_SHIPINSTRUCT]", "[L_SHIPMODE]", "[L_COMMENT]", "[SKIP]"};
	static String[] CWindow = new String[windowSize];
	static String[] Calpha = new String[17];

	public static String[] getCalpha() {
		return Calpha;
	}

	public static void setCalpha(String[] calpha) {
		Calpha = calpha;
	}

	public static String[] getCWindow() {
		return CWindow;
	}

	public static void setCWindow(String[] cWindow) {
		CWindow = cWindow;
	}

	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		Connection conn = SQLServerConnectionFactory.getConnection();
		Statement stmt = conn.createStatement();
		ResultSet rset;
		int checkPointSize = 10, count = 0;
		int index = 20, d = 0;
		String[] ref = new String[17];
		for (int i = 0; i < 17; i++) {
			for (int j = 0; j < 17; j++) {
				affinityMatrix[i][j] = 0;
			}
		}
		while (true) {
	/*
	 * get the new query from database
	 */
			String nextQuery = "SELECT [RowNumber],[TextData] FROM [master].[dbo].[trace221] WHERE rownumber =" + index;
			rset = stmt.executeQuery(nextQuery);
			rset.next();
			String newQuery = rset.getString("TextData");
			debug("1:new query:" + d + newQuery);
			d++;
			if (newQuery == null || ref(newQuery).length == 0) {

				if (ref(newQuery).length == 0) {
					index++;
				}
				continue;
			} else {
				index++;
				debug(newQuery);
				querywindow(newQuery);
				ref = ref(newQuery);
				for (int i = 0; i < ref.length; i++) {
					if (ref != null) {
						System.out.println(ref[i]);
					}
				}
				dynamicOneDCluster(attrOrder, ref, 0);
				count++;
			}

			if (checkPointSize < count) {
				timeStamp = new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime());
				partitionanalyzer(a, attrOrder);
				count = 0;
				partitioningOptimizer(Partition(bestSplitVector, getCalpha()), oldScheme);
				timeStamp1 = new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime());
				int time = timediff(timeStamp, timeStamp1);
				debug("Time taken to analyze:" + time);
				debug("Do you want to continue?");
				Scanner sc = new Scanner(System.in);
				debug("press any button");
				sc.next();
			}
		}
	}

	/*
	 * partitioning optimizer module of AutoStore
	 */
	private static void partitioningOptimizer(String[] partition, String[] oldScheme2) throws SQLException, ClassNotFoundException {
		partition = CurrentUpdate(partition);
		int cost = (int) cost(getCWindow(), Partition(previousSplit, previousAttrOrder));
		int cost2 = (int) cost(getCWindow(), partition);
		debug("old cost:" + cost + "new cost:" + cost2);
		int y = 5;
		y = (int) Math.pow(y, -1 * 6);
		int btransform = cost - cost2;
		btransform = (1 / (1 - y)) * btransform;
		int ctransform = 0;
		Connection conn = SQLServerConnectionFactory.getConnection();
		Statement stmt = conn.createStatement();
		ctransform = 44;//Average of worst case and best case is considered
		debug("btransform:" + btransform + "ctransform:" + ctransform);
		if (btransform > ctransform) {
			previousSplit = bestSplitVector;
			previousAttrOrder = getCalpha();
			materialize(partition);
		}
	}
/*
 * Materialize function materializes the best partition
 */

	private static void materialize(String[] partition) throws SQLException, ClassNotFoundException {
		String initial;
		if (getWhichal() == 1) {
			initial = "a";
			setWhichal(0);
		} else {
			initial = "b";
			setWhichal(1);
		}
		Connection conn = SQLServerConnectionFactory.getConnection();
		Statement stmt2 = conn.createStatement();
		String query, insert;
		int length = 0;
		while (true) {
			length++;
			if (partition[length] == null) {
				break;
			}
		}
		partition = CurrentUpdate(partition);
		for (int i = 0; i < length; i++) {
			query = "IF EXISTS(SELECT 1 FROM sys.objects WHERE OBJECT_ID = OBJECT_ID(N'" + initial + table + i + "') AND type = (N'U')) DROP TABLE " + initial + table + i;
			stmt2.executeUpdate(query);
			query = "Create table dbo." + initial + table + i + "(" + docreate(partition[i]) + ")";
			stmt2.executeUpdate(query);
			insert = "INSERT INTO " + initial + table + i + " SELECT" + " " + partition[i] + " " + "FROM " + table;
			debug(insert);
			stmt2.executeUpdate(insert);
		}
		if (first) {
			String linei = PropertyHelper.getValueFromConfig(KeyConstants.LINEI);
			stmt2.executeUpdate(linei);
		} else {
			String DropView = "DROP VIEW dbo." + table;
			stmt2.executeUpdate(DropView);
		}
		String view = "CREATE VIEW [dbo]." + table + " (" + attributes + ")AS select " + attributes1 + " from " + initial + table + "0" + " T0" + " ";
		for (int i = 1; i < length; i++) {
			int y = i - 1;
			view += " INNER JOIN " + initial + table + i + " T" + i + " ON T" + i + ".[L_OrderKey] =T" + y + ".[L_OrderKey] AND T" + i + ".[L_PartKey] =T" + y + ".[L_PartKey] AND T" + i + ".[L_SuppKey] =  T" + y + ".[L_SuppKey] ";

		}
		stmt2.executeUpdate(view);
		int oldlength = 0;
		while (true) {
			oldlength++;
			if (oldScheme[oldlength] == null) {
				break;
			}
		}
		String drop;
		String oldinitial;
		if (initial.equals("a")) oldinitial = "b";
		else {
			oldinitial = "a";
		}
		if (!first) {
			for (int j = 0; j < oldlength; j++) {
				drop = "drop table " + oldinitial + table + j;
				stmt2.executeUpdate(drop);
			}
		} else {
			first = false;
		}
		for (int i = 0; i < 20; i++) {
			oldScheme[i] = null;
		}
		for (int i = 0; i < length; i++) {
			oldScheme[i] = initial + table + i;
		}
	}

	/*
	 * partition analyzer enumerates over all solutions and then outputs the best partitioning
	 * solution
	 */
	private static int[] partitionanalyzer(String[] window, String[] lalpha) throws SQLException, ClassNotFoundException {
		System.out.println("Analyzing Partitions");
		setCWindow(window);
		setCalpha(lalpha);
		int[] lsplit = new int[16];
		for (int i = 0; i < 16; i++) {
			lsplit = UnsetSplitLine(lsplit, i);
		}
		int q = 0;
		for (int x = 0; x < 17; x++) {
			if (!check(lalpha, x)) {
				lsplit = UnsetSplitLine(lsplit, x);
				q++;
			} else {
				break;
			}
		}
		if (q > 0) {
			lsplit = SetSplitLine(lsplit, q);
		}
		int r = 0;
		for (int y = 16; y >= 0; y--) {
			if (!check(lalpha, y)) {
				if (y < 16) {
					lsplit = UnsetSplitLine(lsplit, y);
				}
				r++;
			} else {
				break;
			}
		}
		if (r > 0) {
			lsplit = SetSplitLine(lsplit, 16 - r);
		}
		minQueryCost = maxQueryCost;
		bfEnumerate(lsplit, q + 1, 16 - r);
		return bestSplitVector;
	}

	/*
	 * bfEnumerate is the function used by partition pruning analyzer after the head and tail were pruned
	 */
	private static void bfEnumerate(int[] lsplit, int x, int y) throws SQLException, ClassNotFoundException {
		int newcost;
		for (int i = x; i <= y; i++) {
			for (int j = x; j <= i - 1; j++) {
				lsplit = UnsetSplitLine(lsplit, j);
			}
			if (i < y) {
				lsplit = SetSplitLine(lsplit, i);
				bfEnumerate(lsplit, i + 1, y);

			} else {
				if (sum(lsplit) >= 5) {
					continue;
				}
				newcost = (int) cost(getCWindow(), Partition(lsplit, getCalpha()));
				if (newcost < minQueryCost) {
					bestSplitVector = lsplit;
					minQueryCost = newcost;
				}
			}
		}
	}

	/*
	 * This is a function used to calculate the no. of partitions.
	 */
	private static int sum(int[] lsplit) {
		int j = 0;
		for (int i = 0; i < 16; i++) {
			j += lsplit[i];
		}
		return j;
	}

	/*
	 * Partition function returns a string array of partitions.
	 */
	private static String[] Partition(int[] lsplit, String[] calpha2) {
		String[] rpart = new String[17];
		rpart[0] = calpha2[0];
		int j = 0;
		for (int i = 0; i < 16; i++) {
			if (lsplit[i] == 0) {
				rpart[j] = rpart[j] + "," + calpha2[i + 1];
			} else {
				j++;
				rpart[j] = "" + calpha2[i + 1];
			}
		}
		return rpart;
	}

	/*
	 * checks if an attribute is referenced by any query in the current query window
	 */
	private static boolean check(String[] lalpha, int x) {
		String l = lalpha[x];
		String[] z = new String[6];
		z = getCWindow();
		for (int k = 0; k < windowSize; k++) {
			String[] q = new String[17];
			q = ref(z[k]);
			for (int j = 0; j < q.length; j++) {
				if (q[j] != null) {
					if (q[j].equals(l)) {
						return true;
					}
				} else {
					return false;
				}
			}
		}
		return false;
	}

	/*
	 * returns all the attrib's referenced by a particular query
	 */
	public static String[] ref(String query) {
		String query1 = PropertyHelper.getValueFromConfig(KeyConstants.QUERY1);
		String query2 = PropertyHelper.getValueFromConfig(KeyConstants.QUERY2);
		String query3 = PropertyHelper.getValueFromConfig(KeyConstants.QUERY3);
		String query4 = PropertyHelper.getValueFromConfig(KeyConstants.QUERY4);
		String query5 = PropertyHelper.getValueFromConfig(KeyConstants.QUERY5);
		String query6 = PropertyHelper.getValueFromConfig(KeyConstants.QUERY6);
		String query7 = PropertyHelper.getValueFromConfig(KeyConstants.QUERY7);
		String query8 = PropertyHelper.getValueFromConfig(KeyConstants.QUERY8);
		String query9 = PropertyHelper.getValueFromConfig(KeyConstants.QUERY9);
		String query10 = PropertyHelper.getValueFromConfig(KeyConstants.QUERY10);
		String query11 = PropertyHelper.getValueFromConfig(KeyConstants.QUERY11);
		String query12 = PropertyHelper.getValueFromConfig(KeyConstants.QUERY12);
		String query13 = PropertyHelper.getValueFromConfig(KeyConstants.QUERY13);
		String query14 = PropertyHelper.getValueFromConfig(KeyConstants.QUERY14);
		String query15 = PropertyHelper.getValueFromConfig(KeyConstants.QUERY15);
		String query16 = PropertyHelper.getValueFromConfig(KeyConstants.QUERY16);
		String query17 = PropertyHelper.getValueFromConfig(KeyConstants.QUERY17);
		String query18 = PropertyHelper.getValueFromConfig(KeyConstants.QUERY18);
		String query19 = PropertyHelper.getValueFromConfig(KeyConstants.QUERY19);
		String query20 = PropertyHelper.getValueFromConfig(KeyConstants.QUERY20);
		String query21 = PropertyHelper.getValueFromConfig(KeyConstants.QUERY21);
		String query22 = PropertyHelper.getValueFromConfig(KeyConstants.QUERY22);
		String[] quer1 = {"[L_RETURNFLAG]", "[L_LINESTATUS]", "[L_QUANTITY]", "[L_EXTENDEDPRICE]", "[L_DISCOUNT]", "[L_TAX]", "[L_SHIPDATE]", "[L_LINESTATUS]"};
		String[] quer2 = {};
		String[] quer3 = {"[L_ORDERKEY]", "[L_EXTENDEDPRICE]", "[L_DISCOUNT]", "[L_SHIPDATE]"};
		String[] quer4 = {"[L_ORDERKEY]", "[L_COMMITDATE]", "[L_RECEIPTDATE]"};
		String[] quer5 = {"[L_EXTENDEDPRICE]", "[L_DISCOUNT]", "[L_SUPPKEY]", "[L_ORDERKEY]"};
		String[] quer6 = {"[L_EXTENDEDPRICE]", "[L_DISCOUNT]", "[L_SHIPDATE]", "[L_QUANTITY]"};
		String[] quer7 = {"[L_SHIPDATE]", "[L_EXTENDEDPRICE]", "[L_DISCOUNT]", "[L_SUPPKEY]", "[L_ORDERKEY]"};
		String[] quer8 = {"[L_EXTENDEDPRICE]", "[L_DISCOUNT]", "[L_PARTKEY]", "[L_SUPPKEY]", "[L_ORDERKEY]"};
		String[] quer9 = {"[L_EXTENDEDPRICE]", "[L_DISCOUNT]", "[L_QUANTITY]", "[L_SUPPKEY]", "[L_PARTKEY]", "[L_ORDERKEY]"};
		String[] quer10 = {"[L_EXTENDEDPRICE]", "[L_DISCOUNT]", "[L_ORDERKEY]", "[L_RETURNFLAG]"};
		String[] quer11 = {};
		String[] quer12 = {"[L_SHIPMODE]", "[L_ORDERKEY]", "[L_COMMITDATE]", "[L_RECEIPTDATE]", "[L_SHIPDATE]"};
		String[] quer13 = {};
		String[] quer14 = {"[L_EXTENDEDPRICE]", "[L_DISCOUNT]", "[L_PARTKEY]", "[L_SHIPDATE]"};
		String[] quer15 = {};
		String[] quer16 = {};
		String[] quer17 = {"[L_EXTENDEDPRICE]", "[L_PARTKEY]", "[L_QUANTITY]"};
		String[] quer18 = {"[L_ORDERKEY]", "[L_QUANTITY]"};
		String[] quer19 = {"[L_EXTENDEDPRICE]", "[L_DISCOUNT]", "[L_PARTKEY]", "[L_QUANTITY]", "[L_SHIPMODE]", "[L_SHIPINSTRUCT]"};
		String[] quer20 = {"[L_QUANTITY]", "[L_PARTKEY]", "[L_SUPPKEY]", "[L_SHIPDATE]"};
		String[] quer21 = {"[L_SUPPKEY]", "[L_ORDERKEY]", "[L_RECEIPTDATE]", "[L_COMMITDATE]"};
		String[] quer22 = {};

		if (query.equals(query1)) return quer1;
		else if (query.equals(query2)) return quer2;
		else if (query.equals(query3)) return quer3;
		else if (query.equals(query4)) return quer4;
		else if (query.equals(query5)) return quer5;
		else if (query.equals(query6)) return quer6;
		else if (query.equals(query7)) return quer7;
		else if (query.equals(query8)) return quer8;
		else if (query.equals(query9)) return quer9;
		else if (query.equals(query10)) return quer10;
		else if (query.equals(query11)) return quer11;
		else if (query.equals(query12)) return quer12;
		else if (query.equals(query13)) return quer13;
		else if (query.equals(query14)) return quer14;
		else if (query.equals(query15)) return quer15;
		else if (query.equals(query16)) return quer16;
		else if (query.equals(query17)) return quer17;
		else if (query.equals(query18)) return quer18;
		else if (query.equals(query19)) return quer19;
		else if (query.equals(query20)) return quer20;
		else if (query.equals(query21)) return quer21;
		else if (query.equals(query22)) return quer22;
		else return quer22;
	}

	public static int[] UnsetSplitLine(int[] S, int j) {
		S[j] = 0;
		return S;
	}

	public static int[] SetSplitLine(int[] S, int i) {
		S[i] = 1;
		return S;
	}

	/*
	 * maintains query window
	 */
	private static void querywindow(String newquery) {
		if (windowIndex >= windowSize) {
			for (int i = 0; i < windowSize - 1; i++) {

				a[i] = a[i + 1];
			}
			windowIndex = windowSize - 1;
		}
		a[windowIndex] = newquery;
		windowIndex++;
	}

	public static double cost(String[] workload, String[] CurrentScheme) throws SQLException, ClassNotFoundException {
		debug("cost:");
		Connection conn = SQLServerConnectionFactory.getConnection();
		Statement stmt2 = conn.createStatement();
		String query, insert;
		int length = 0;
		while (true) {
			length++;
			if (CurrentScheme[length] == null) {
				break;
			}
		}
		debug("Length partitions:" + length);
		CurrentScheme = CurrentUpdate(CurrentScheme);
		for (int i = 0; i < length; i++) {
			query = "IF EXISTS(SELECT 1 FROM sys.objects WHERE OBJECT_ID = OBJECT_ID(N'" + table + i + "') AND type = (N'U')) DROP TABLE " + table + i;
			stmt2.executeUpdate(query);
			query = "Create table dbo." + table + i + " (" + docreate(CurrentScheme[i]) + ")";
			debug("this will sql:" + query);
			stmt2.executeUpdate(query);
			insert = "INSERT INTO " + table + i + " SELECT" + " " + CurrentScheme[i] + " " + "FROM [autostore].[dbo]." + table;
			debug("insert:" + insert);
			stmt2.executeUpdate(insert);
		}
		int oldlength = 0;
		while (true) {
			oldlength++;
			if (oldScheme[oldlength] == null) {
				break;
			}

		}
		for (int i = 0; i < oldlength; i++) {
			String rename;
			if (initial) {
				initial = false;
				oldScheme[0] = "lineitem";
			}
			rename = "sp_rename 'dbo." + oldScheme[i] + "','" + oldScheme[i] + "x'";
			debug(rename);
			stmt2.executeUpdate(rename);
		}

		int cost = 0;//later edit this
		cost = getCost(workload, CurrentScheme);
		String DropView = "DROP VIEW dbo." + table;
		String drop;
		debug("table droppings");
		for (int j = 0; j < oldlength; j++) {
			drop = "drop table " + table + j;
			stmt2.executeUpdate(drop);
		}
		for (int i = 0; i < oldlength; i++) {
			String rename;
			rename = "sp_rename 'dbo." + oldScheme[i] + "x','" + oldScheme[i] + "'";
			debug(rename);
			stmt2.executeUpdate(rename);
		}
		return cost;
	}

	/*
	 * gets estimated cost from the optimizer.
	 */
	private static int getCost(String[] workload, String[] currentScheme) throws SQLException, ClassNotFoundException {
		double timeIOSQL = Double.parseDouble("0");
		double timeCPUSQL = Double.parseDouble("0");
		int clength;
		clength = getlength(currentScheme);
		String[] attrib = new String[clength];
		for (int i = 0; i < clength; i++) {
			attrib[i] = "";
		}
		for (int i = 0; i < workload.length; i++) {
			String[] refer = ref(workload[i]);
			for (int j = 0; j < refer.length; j++) {
				for (int k = 0; k < clength; k++) {
					if (currentScheme[k].contains(refer[j])) {
						attrib[k] += refer[j] + ",";
					}
				}
			}

			for (int j = 0; j < clength; j++) {
				Connection conn = SQLServerConnectionFactory.getConnection();
				Statement stmt23 = conn.createStatement();
				stmt23.executeUpdate("SET SHOWPLAN_ALL ON;");
				if (attrib[j] != null && !attrib[j].equals("")) {
					ResultSet rset = stmt23.executeQuery("SELECT " + update(attrib[j]) + " FROM [autostore].[dbo]." + table + j);
					while (rset.next()) {
						if (rset.getString("EstimateIO") != null && rset.getString("EstimateCPU") != null) {
							timeIOSQL += Double.parseDouble(rset.getString("EstimateIO"));
							debug(rset.getString("EstimateIO"));
							timeCPUSQL += Double.parseDouble(rset.getString("EstimateCPU"));
						}
					}
					stmt23.executeUpdate("SET SHOWPLAN_ALL OFF;");
				} else {
					continue;
				}
			}
			for (int g = 0; g < clength; g++) {
				attrib[g] = "";
			}
		}
		debug("*****************************Costs: " + timeIOSQL + " CPU:" + timeCPUSQL);
		return (int) timeIOSQL;
	}

	/*
	 * update util function
	 */
	private static String update(String string) {
		String k = "";
		int length = string.length();
		for (int i = 0; i < length - 1; i++) {
			k += string.charAt(i);
		}
		return k;
	}

	/*
	 * gets the length of a string array
	 */
	private static int getlength(String[] currentScheme) {
		int filled = 0;
		for (int i = 0; i < currentScheme.length; i++) {

			if (currentScheme[filled] == null) break;
			filled++;
		}
		return filled;
	}

	static int Clength = 0;

	/*
	 * Currnet Update function updates the current solution such that every partition has
	 * primary key.
	 */
	public static String[] CurrentUpdate(String[] a) {
		Clength = 0;
		while (true) {
			Clength++;
			if (a[Clength] == null) {
				break;
			}
		}
		for (int i = 0; i < Clength; i++) {
			if (!a[i].contains("[L_ORDERKEY]")) {
				a[i] = a[i] + ",[L_ORDERKEY]";
			}
			if (!a[i].contains("[L_PARTKEY]")) {
				a[i] = a[i] + ",[L_PARTKEY]";
			}
			if (!a[i].contains("[L_SUPPKEY]")) {
				a[i] = a[i] + ",[L_SUPPKEY]";
			}
			debug("" + a[i]);
		}
		return a;
	}

	/*
	 * Contribution calculation function used on affinityMatrix matrix
	 */
	public static int c(String leftPartUnit, String rightPartUnit, String newUnit) {
		int contrib = 0, ab = 0;

		int i, j, k;
		if (left) {
			j = getInteger(rightPartUnit);
			k = getInteger(newUnit);
			for (int z = 0; z < 17; z++) {
				ab = getInteger(GetPartUnit(attrOrder, z));
				contrib = contrib + affinityMatrix[ab][k] * affinityMatrix[ab][j];
			}
			contrib = 2 * contrib;
			return contrib;
		} else if (right) {
			i = getInteger(leftPartUnit);
			k = getInteger(newUnit);
			for (int z = 0; z < 17; z++) {
				ab = getInteger(GetPartUnit(attrOrder, z));
				contrib = contrib + (affinityMatrix[ab][i] * affinityMatrix[ab][k]);
			}
			contrib = 2 * contrib;
			return contrib;
		} else {
			i = getInteger(leftPartUnit);
			j = getInteger(rightPartUnit);
			k = getInteger(newUnit);
			for (int z = 0; z < 17; z++) {
				ab = getInteger(GetPartUnit(attrOrder, z));
				contrib = contrib + (affinityMatrix[ab][i] * affinityMatrix[ab][k] + affinityMatrix[ab][k] * affinityMatrix[ab][j] + affinityMatrix[ab][i] * affinityMatrix[ab][j]);
			}
			contrib = 2 * contrib;
			return contrib;
		}
	}
/*
 * get integer code of an attribute
 */

	/**
	 * @param attribute
	 * @return
	 */
	public static int getInteger(String attribute) {

		if (attribute.equals("[L_ORDERKEY]")) return 0;
		else if (attribute.equals("[L_PARTKEY]")) return 1;
		else if (attribute.equals("[L_SUPPKEY]")) return 2;
		else if (attribute.equals("[L_LINENUMBER]")) return 3;
		else if (attribute.equals("[L_QUANTITY]")) return 4;
		else if (attribute.equals("[L_EXTENDEDPRICE]")) return 5;
		else if (attribute.equals("[L_DISCOUNT]")) return 6;
		else if (attribute.equals("[L_TAX]")) return 7;
		else if (attribute.equals("[L_RETURNFLAG]")) return 8;
		else if (attribute.equals("[L_LINESTATUS]")) return 9;
		else if (attribute.equals("[L_SHIPDATE]")) return 10;
		else if (attribute.equals("[L_COMMITDATE]")) return 11;
		else if (attribute.equals("[L_RECEIPTDATE]")) return 12;
		else if (attribute.equals("[L_SHIPINSTRUCT]")) return 13;
		else if (attribute.equals("[L_SHIPMODE]")) return 14;
		else if (attribute.equals("[L_COMMENT]")) return 15;
		else if (attribute.equals("[SKIP]")) return 16;
		else {
			return 99;
		}
	}

	/*
	 * get type of an attribute used for creating a create statement
	 */
	public static String getType(String attribute) {

		if (attribute.equals("[L_ORDERKEY]")) return "int";
		else if (attribute.equals("[L_PARTKEY]")) return "int";
		else if (attribute.equals("[L_SUPPKEY]")) return "int";
		else if (attribute.equals("[L_LINENUMBER]")) return "int";
		else if (attribute.equals("[L_QUANTITY]")) return "int";
		else if (attribute.equals("[L_EXTENDEDPRICE]")) return "decimal(13,2)";
		else if (attribute.equals("[L_DISCOUNT]")) return "decimal(13,2)";
		else if (attribute.equals("[L_TAX]")) return "decimal(13,2)";
		else if (attribute.equals("[L_RETURNFLAG]")) return "varchar(64)";
		else if (attribute.equals("[L_LINESTATUS]")) return "varchar(64)";
		else if (attribute.equals("[L_SHIPDATE]")) return "datetime";
		else if (attribute.equals("[L_COMMITDATE]")) return "datetime";
		else if (attribute.equals("[L_RECEIPTDATE]")) return "datetime";
		else if (attribute.equals("[L_SHIPINSTRUCT]")) return "varchar(64)";
		else if (attribute.equals("[L_SHIPMODE]")) return "varchar(64)";
		else if (attribute.equals("[L_COMMENT]")) return "varchar(64)";
		else if (attribute.equals("[SKIP]")) return "varchar(64)";
		else {
			return "wrong";
		}
	}

	/*
	 * helper in create statement
	 */
	public static String docreate(String samp) {
		char c;
		String dear = "";
		String full = "";
		for (int i = 0; i < samp.length(); i++) {
			c = samp.charAt(i);
			if (c != ',' && i != (samp.length() - 1)) {
				dear = dear + c;
			} else {
				if (i == samp.length() - 1) dear = dear + c;
				full = full + dear + " " + getType(dear);
				dear = "";
				if (i != samp.length() - 1) {
					full += ",";
				}
			}
		}
		return full;

	}

	public static String GetPartUnit(String[] ref, int q) {
		return ref[q];
	}

	public static int GetPartUnitPos(String[] alpha, String newPart) {
		for (int k = 0; k < 17; k++) {
			if (alpha[k].equals(newPart)) {
				return k;
			}
		}
		return 99;
	}

	private static void debug(String length) {
		System.out.println(length);
	}

	public static void PutPartUnitPos(String[] alpha, String newPartUnit, int l) {
		int gp = GetPartUnitPos(alpha, newPartUnit);
		if (gp < l) {
			for (int i = gp; i < l; i++) {
				alpha[i] = alpha[i + 1];
			}
			alpha[l] = newPartUnit;
		} else if (gp > l) {

			for (int i = gp; i > l; i--) {
				alpha[i] = alpha[i - 1];
			}
			alpha[l] = newPartUnit;
		} else {
			alpha[l] = newPartUnit;
		}
		AutoStore.attrOrder = alpha;
	}

	/*
	 * This is the Partitioning unit clusterer module of autostore
	 */
	public static void dynamicOneDCluster(String[] alpha, String[] ref, int i) {
		int l = 0, contribution, position, position1, contribution1;
		String newPartUnit = GetPartUnit(ref, i);
		if (i == 0) {
			l = GetPartUnitPos(alpha, newPartUnit);
			PutPartUnitPos(alpha, newPartUnit, l);
			dynamicOneDCluster(alpha, ref, i + 1);
		} else {
			contribution = 0;
			position = 0;
			for (int k = 0; k < i - 1; k++) {
				String currPartUnit = GetPartUnit(ref, k);
				String rightPartUnit = "", leftPartUnit = "";
				l = GetPartUnitPos(alpha, currPartUnit);
				if (l != 0) {
					leftPartUnit = GetPartUnit(alpha, l - 1);
				} else {
					left = true;
				}
				if (l != 16) {
					rightPartUnit = GetPartUnit(alpha, l + 1);
				} else {
					right = true;
				}
				contribution1 = c(leftPartUnit, currPartUnit, newPartUnit);
				position1 = 1;
				if (c(currPartUnit, rightPartUnit, newPartUnit) > contribution1) {
					contribution1 = c(currPartUnit, rightPartUnit, newPartUnit);
					position1 = l + 1;
				}
				if (contribution1 > contribution) {
					contribution = contribution1;
					position = position1;
				}
			}
			PutPartUnitPos(alpha, newPartUnit, position);
			if (i < ref.length - 1) {
				dynamicOneDCluster(alpha, ref, i + 1);
			}

		}
	}

	public static int timediff(String T1, String T2) {
		SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
		long diffHours = 0, diffMinutes = 0, diffSeconds = 0, diff = 0;
		Date d1 = null;
		Date d2 = null;

		try {
			d1 = format.parse(T1);
			d2 = format.parse(T2);
			diff = d2.getTime() - d1.getTime();

			diffSeconds = diff / 1000 % 60;
			diffMinutes = diff / (60 * 1000) % 60;
			diffHours = diff / (60 * 60 * 1000) % 24;
			System.out.print(diffHours + " hours, ");
			System.out.print(diffMinutes + " minutes, ");
			System.out.print(diffSeconds + " seconds.");

		} catch (Exception e) {
			e.printStackTrace();
		}
		return (int) diff / 1000;
	}
}