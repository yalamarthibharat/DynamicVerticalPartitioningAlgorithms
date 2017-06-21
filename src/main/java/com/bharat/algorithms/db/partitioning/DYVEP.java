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
 * This is com.bharat.propertyhelper.ClusteredAffinityMatrix code for TPC-H data
 */
public class DYVEP {
	public static int[][] QT = new int[19][17];
	public static int index = 29, pivot;
	public static int[] freq = new int[19];
	public static int checkpoint = 0;
	public static String[] empty = {};
	public static int[] attrarray = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
	public static String[] attrorder = {"[L_ORDERKEY]", "[L_PARTKEY]", "[L_SUPPKEY]", "[L_LINENUMBER]", "[L_QUANTITY]", "[L_EXTENDEDPRICE]", "[L_DISCOUNT]", "[L_TAX]", "[L_RETURNFLAG]", "[L_LINESTATUS]", "[L_SHIPDATE]", "[L_COMMITDATE]", "[L_RECEIPTDATE]", "[L_SHIPINSTRUCT]", "[L_SHIPMODE]", "[L_COMMENT]", "[SKIP]"};
	public static int whichal = 0;
	static String table = PropertyHelper.getValueFromConfig(KeyConstants.TABLE);
	public static String attributes = PropertyHelper.getValueFromConfig(KeyConstants.ATTRIBUTES);
	public static String attributes1 = PropertyHelper.getValueFromConfig(KeyConstants.ATTRIBUTES1);
	static String[] OldScheme = new String[20];
	public static boolean left = false, First = true, right = false, initial = true;
	public static int zmax = 0;
	static String timeStamp;
	static String timeStamp1;

	public static int getWhichal() {
		return whichal;
	}

	public static void setWhichal(int whichal) {
		DYVEP.whichal = whichal;
	}

	public static void main(String[] args) throws Exception {
		ResultSet rset;
		Connection conn = SQLServerConnectionFactory.getConnection();
		Statement stmt = conn.createStatement();
		initializeqt();
		debug("-----------------QT-------------------");
		debug(QT);
		for (int i = 0; i < freq.length; i++) {
			freq[i] = 0;
		}
		freq[16] = 1;
		freq[17] = 1;
		freq[18] = 1;
		while (true) {
			/*
			 * Gets next query from the trace table
			 */
			String nextquery = "SELECT [RowNumber],[TextData] FROM [master].[dbo].[trace221] WHERE rownumber =" + index;
			rset = stmt.executeQuery(nextquery);
			rset.next();
			String newquery = rset.getString("TextData");
			if (newquery == null || ref(newquery).length == 0) {
				if (newquery == null) {
					continue;
				}
				if (ref(newquery).length == 0) {
					index++;
				}
				continue;
			} else {
				checkpoint++;
				index++;
				if (getQueryno1(newquery) != 99) {
					freq[getQueryno1(newquery)]++;
				}
				if (checkpoint > 7) {
					timeStamp = new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime());
					checkpoint = 0;
					ClusteredAffinityMatrix.setSites(freq);
					ClusteredAffinityMatrix.setClustering1(QT);
					ClusteredAffinityMatrix.main(empty);
					int[][] test1 = ClusteredAffinityMatrix.getAttribute();
					int[][] test2 = ClusteredAffinityMatrix.getClustering();
					int[] temparray = getnattribute(test1, test2);
					for (int x = 0; x < temparray.length; x++) {
						temparray[x] = attrarray[temparray[x]];
					}
					attrarray = temparray;
					debug("Attribute array: ----------------------");
					debug(attrarray);
					attrorder = getneworder();
					pivot = getbestpart(attrarray);
					String[] a = makepart(pivot);
					if (pivot != 0) {
						materialize(a);
					}
					timeStamp1 = new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime());
					int time = timediff(timeStamp, timeStamp1);
					debug("Time taken to analyze:" + time);
					debug("do you want to continue?");
					Scanner sc = new Scanner(System.in);
					debug("press any button");
					sc.next();
				}
			}
		}
	}

	static int Clength = 0;

	/*
	 * Updates the partitioning solution such that every partition has a primary key associated with it. 
	 */
	public static String[] CurrentUpdate(String[] a) {
		Clength = 2;
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

	/*
	 * make part partition the attribute ordering depending on the pivot.
	 */
	private static String[] makepart(int pivot2) {
		String[] local = new String[2];
		int j = 0;
		local[0] = "";
		local[1] = "";
		for (int i = 0; i < 17; i++) {
			if (i < pivot) {
				local[0] += attrorder[i];
				if (i < pivot - 1) {
					local[0] += ",";
				}
			} else {
				local[1] += attrorder[i];
				if (i < 16) {
					local[1] += ",";
				}
			}
		}
		return local;
	}

	/*
	 * materialize function materializes the solution passed as argument
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
		int length = 2;
		partition = CurrentUpdate(partition);
		debug(partition);
		for (int i = 0; i < length; i++) {
			query = "IF EXISTS(SELECT 1 FROM sys.objects WHERE OBJECT_ID = OBJECT_ID(N'" + initial + table + i + "') AND type = (N'U')) DROP TABLE " + initial + table + i;
			stmt2.executeUpdate(query);
			query = "Create table dbo." + initial + table + i + "(" + docreate(partition[i]) + ")";
			stmt2.executeUpdate(query);
			insert = "INSERT INTO " + initial + table + i + " SELECT" + " " + partition[i] + " " + "FROM " + table;
			debug(insert);
			stmt2.executeUpdate(insert);
		}
		if (First) {
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
			if (OldScheme[oldlength] == null) {
				break;
			}
		}
		String drop;
		String oldinitial;
		if (initial.equals("a")) oldinitial = "b";
		else {
			oldinitial = "a";
		}
		if (!First) {
			for (int j = 0; j < oldlength; j++) {
				drop = "drop table " + oldinitial + table + j;
				stmt2.executeUpdate(drop);
			}
		} else {
			First = false;
		}
		for (int i = 0; i < 20; i++) {
			OldScheme[i] = null;
		}
		for (int i = 0; i < length; i++) {
			OldScheme[i] = initial + table + i;
		}
	}

	/*
	 * debug function for debugging
	 */
	private static void debug(String[] partition) {
		for (int i = 0; i < 2; i++) {
			debug(partition[i]);
		}
	}

	/*
	 * this returns the new order of attributes in CAT( as they are permuted)
	 */
	private static String[] getneworder() {
		String[] local = new String[17];
		for (int i = 0; i < 17; i++) {
			local[i] = getattrname(attrarray[i]);
		}
		return local;
	}

	private static void debug(String string, int[] freq2) {
		debug(freq2);
	}

	private static void debug(String string, int[][] freq2) {
		debug(freq2);
	}
	/*
	 * Get best part returns the partition with Z max ( navathe's BVP)
	 */

	private static int getbestpart(int[] attrarray2) {
		boolean upper = false, lower = false;
		int cut = 0, clt = 0, cit = 0, p = 0;
		int z;
		for (int i = 1; i < 17; i++) {
			cut = clt = cit = 0;
			upper = lower = false;
			debug("this is pivot at:" + i);
			for (int q = 0; q < 19; q++) {
				upper = lower = false;
				for (int u = 0; u < i; u++) {
					if (QT[q][attrarray[u]] == 1) {
						upper = true;
					}
				}
				for (int l = i; l < 17; l++) {
					if (QT[q][attrarray[l]] == 1) {
						lower = true;
					}
				}
				if (upper && lower) {
					cit += freq[q];
				} else if (upper && !lower) {
					cut += freq[q];
				} else if (lower && !upper) {
					clt += freq[q];
				}
			}
			debug("cut:" + cut + "clt:" + clt + "cit:" + cit);
			z = (cut * clt) - (cit * cit);
			debug(z + " " + zmax);
			if (z > zmax) {
				zmax = z;
				p = i;
			}
		}
		return p;
	}

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

	/*
	 * attribute type is returned
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
	 * returns attrib ordering
	 */
	private static int[] getnattribute(int[][] Q, int[][] W) {
		debug("attrarray", attrarray);
		int[] newAttrOrder = new int[17];
		int p = 0, q = 0, v = 0, z = 0;
		int[] oldrowvalue = new int[17];
		int[] newrowvalue = new int[17];
		int[] zeros1 = new int[17];

		for (int o = 0; o < newAttrOrder.length; o++) {
			newAttrOrder[o] = 99;
		}
		for (int i = 0; i < Q.length; i++) {
			for (int j = 0; j < Q[i].length; j++) {
				oldrowvalue[p] += Q[i][j];
			}
			if (oldrowvalue[p] == 0) {
				zeros1[z] = p;
				z++;
			}
			p++;
		}
		for (int i = 0; i < W.length; i++) {
			for (int j = 0; j < W[i].length; j++) {
				newrowvalue[q] += W[i][j];

			}
			q++;
		}
		int r = 0;
		for (int i = 0; i < newrowvalue.length; i++) {
			if (newrowvalue[i] == 0) {
				newAttrOrder[i] = zeros1[r];
				r++;
			}
		}
		boolean[] track = new boolean[newrowvalue.length];
		for (int b = 0; b < newrowvalue.length; b++) {
			track[b] = false;
		}
		for (int i = 0; i < oldrowvalue.length; i++) {
			out:
			for (int j = 0; j < newrowvalue.length; j++) {
				if (oldrowvalue[i] == newrowvalue[j]) {
					if (!track[j]) {
						track[j] = true;
						if (newAttrOrder[j] == 99) {
							newAttrOrder[j] = v;
							v++;
							break out;
						}
						v++;
						break out;
					} else {
						continue;
					}
				}
			}
		}
		debug("ordering", newAttrOrder);
		return newAttrOrder;
	}

	public static String getattrname(int number) {
		switch (number) {
			case 0:
				return "[L_ORDERKEY]";
			case 1:
				return "[L_PARTKEY]";
			case 2:
				return "[L_SUPPKEY]";
			case 3:
				return "[L_LINENUMBER]";
			case 4:
				return "[L_QUANTITY]";
			case 5:
				return "[L_EXTENDEDPRICE]";
			case 6:
				return "[L_DISCOUNT]";
			case 7:
				return "[L_TAX]";
			case 8:
				return "[L_RETURNFLAG]";
			case 9:
				return "[L_LINESTATUS]";
			case 10:
				return "[L_SHIPDATE]";
			case 11:
				return "[L_COMMITDATE]";
			case 12:
				return "[L_RECEIPTDATE]";
			case 13:
				return "[L_SHIPINSTRUCT]";
			case 14:
				return "[L_SHIPMODE]";
			case 15:
				return "[L_COMMENT]";
			case 16:
				return "[SKIP]";
		}
		return null;
	}

	private static void initializeqt() {
		for (int a = 0; a < QT.length; a++) {
			for (int b = 0; b < QT[a].length; b++) {
				QT[a][b] = 0;
			}
		}
		QT[0][8] = 1;
		QT[0][9] = 1;
		QT[0][4] = 1;
		QT[0][5] = 1;
		QT[0][6] = 1;
		QT[0][7] = 1;
		QT[0][10] = 1;
		QT[1][0] = 1;
		QT[1][5] = 1;
		QT[1][6] = 1;
		QT[1][10] = 1;
		QT[2][0] = 1;
		QT[2][11] = 1;
		QT[2][12] = 1;
		QT[3][5] = 1;
		QT[3][6] = 1;
		QT[3][2] = 1;
		QT[3][0] = 1;
		QT[4][5] = 1;
		QT[4][6] = 1;
		QT[4][10] = 1;
		QT[4][4] = 1;
		QT[5][10] = 1;
		QT[5][5] = 1;
		QT[5][6] = 1;
		QT[5][2] = 1;
		QT[5][0] = 1;
		QT[6][5] = 1;
		QT[6][6] = 1;
		QT[6][1] = 1;
		QT[6][2] = 1;
		QT[6][0] = 1;
		QT[7][6] = 1;
		QT[7][5] = 1;
		QT[7][4] = 1;
		QT[7][2] = 1;
		QT[7][1] = 1;
		QT[7][0] = 1;
		QT[8][5] = 1;
		QT[8][6] = 1;
		QT[8][0] = 1;
		QT[8][8] = 1;
		QT[9][14] = 1;
		QT[9][0] = 1;
		QT[9][11] = 1;
		QT[9][12] = 1;
		QT[9][10] = 1;
		QT[10][5] = 1;
		QT[10][6] = 1;
		QT[10][1] = 1;
		QT[10][10] = 1;
		QT[11][5] = 1;
		QT[11][1] = 1;
		QT[11][4] = 1;
		QT[12][0] = 1;
		QT[12][4] = 1;
		QT[13][5] = 1;
		QT[13][6] = 1;
		QT[13][1] = 1;
		QT[13][4] = 1;
		QT[13][14] = 1;
		QT[13][13] = 1;
		QT[14][4] = 1;
		QT[14][1] = 1;
		QT[14][2] = 1;
		QT[14][10] = 1;
		QT[15][2] = 1;
		QT[15][0] = 1;
		QT[15][12] = 1;
		QT[15][11] = 1;
		QT[16][3] = 1;
		QT[17][15] = 1;
		QT[18][16] = 1;
		//debug("QT : -------------------------------------------");
		//debug(QT);
	}

	public static void debug(String s) {
		System.out.println(s);
	}

	public static void debug(int s) {
		System.out.println(s);
	}

	public static void debug(int[][] s) {
		debug("matrix");
		for (int i = 0; i < s.length; i++) {
			debug(i);
			for (int j = 0; j < s[i].length; j++) {
				System.out.print(s[i][j] + "");
			}
			System.out.println();
		}
	}

	public static void debug(int[] s) {
		debug("matrix");
		for (int i = 0; i < s.length; i++) {
			System.out.print(s[i] + "\n");
		}
	}

	/*
	 * get the query no.
	 */
	public static int getQueryno1(String query) {

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

		if (query.equals(query1)) return 0;
		else if (query.equals(query2)) ;
		else if (query.equals(query3)) return 1;
		else if (query.equals(query4)) return 2;
		else if (query.equals(query5)) return 3;
		else if (query.equals(query6)) return 4;
		else if (query.equals(query7)) return 5;
		else if (query.equals(query8)) return 6;
		else if (query.equals(query9)) return 7;
		else if (query.equals(query10)) return 8;
		else if (query.equals(query11)) ;
		else if (query.equals(query12)) return 9;
		else if (query.equals(query13)) ;
		else if (query.equals(query14)) return 10;
		else if (query.equals(query15)) ;
		else if (query.equals(query16)) ;
		else if (query.equals(query17)) return 11;
		else if (query.equals(query18)) return 12;
		else if (query.equals(query19)) return 13;
		else if (query.equals(query20)) return 14;
		else if (query.equals(query21)) return 15;
		else if (query.equals(query22)) ;
		return 99;

	}

	/*
	 * ref returns the attribs referenced by the query
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

		if (query.equals(query1) || query.equals("query1")) return quer1;
		else if (query.equals(query2) || query.equals("query2")) return quer2;
		else if (query.equals(query3) || query.equals("query3")) return quer3;
		else if (query.equals(query4) || query.equals("query4")) return quer4;
		else if (query.equals(query5) || query.equals("query5")) return quer5;
		else if (query.equals(query6) || query.equals("query6")) return quer6;
		else if (query.equals(query7) || query.equals("query7")) return quer7;
		else if (query.equals(query8) || query.equals("query8")) return quer8;
		else if (query.equals(query9) || query.equals("query9")) return quer9;
		else if (query.equals(query10) || query.equals("query10")) return quer10;
		else if (query.equals(query11) || query.equals("query11")) return quer11;
		else if (query.equals(query12) || query.equals("query12")) return quer12;
		else if (query.equals(query13) || query.equals("query13")) return quer13;
		else if (query.equals(query14) || query.equals("query14")) return quer14;
		else if (query.equals(query15) || query.equals("query15")) return quer15;
		else if (query.equals(query16) || query.equals("query16")) return quer16;
		else if (query.equals(query17) || query.equals("query17")) return quer17;
		else if (query.equals(query18) || query.equals("query18")) return quer18;
		else if (query.equals(query19) || query.equals("query19")) return quer19;
		else if (query.equals(query20) || query.equals("query20")) return quer20;
		else if (query.equals(query21) || query.equals("query21")) return quer21;
		else if (query.equals(query22) || query.equals("query22")) return quer22;
		else return quer22;
	}
	 /*
	  * get integer code for an attribute
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
}