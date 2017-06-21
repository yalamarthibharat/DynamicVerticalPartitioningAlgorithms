package com.bharat.algorithms.db.partitioning;

import java.util.StringTokenizer;
import java.util.Vector;

/*
 * This code is for TPC-H Benchmark Data .
 * This contains the code for calculating affinityMatrix matrix and Clustered Affinity Matrix
 * The clustered affinityMatrix matrix is returned by this program when called from main program
 */
public class ClusteredAffinityMatrix {
	private static int att = 17;
	private static int noq = 19, s;
	private static int[][] clustering = new int[att][att];
	private static int[][] clustering1 = new int[noq][att];
	private static int[][] attribute = new int[att][att];

	public static void setClustering1(int[][] clustering1) {
		ClusteredAffinityMatrix.clustering1 = clustering1;
	}

	private static int[] sites = new int[noq];
	;
	private static int index;
	private static int[] array;
	private static int b;
	private static StringTokenizer st;

	public static void main(String[] args) throws Exception {
		int i = 0, j = 0, k;
		j = 0;
		for (k = 0; k < att; k++) {
			for (j = 0; j < att; j++) {
				for (i = 0; i < noq; i++) {
					// Checking the condition and computing the affinities
					if (clustering1[i][k] == 1 && clustering1[i][j] == 1)
						b += sites[i];
				}
				// Calculating and assigning values to AAT
				attribute[k][j] = b;
				b = 0;
			}
		}
		System.out.println("\nThe attribute affinityMatrix matrix is :");
		for (i = 0; i < att; i++) {
			for (j = 0; j < att; j++) {
				System.out.print(attribute[i][j] + "  ");
			}
			System.out.println();
		}
		ClusteredAffinityMatrix dy = new ClusteredAffinityMatrix();
		dy.run();
		System.out.println("\nThe clustered affinityMatrix matrix is : ");
		for (i = 0; i < att; i++) {
			for (j = 0; j < att; j++) {
				System.out.print(clustering[i][j] + "  ");
			}
			System.out.println();
		}
	}

	public static int[][] getAttribute() {
		return attribute;
	}

	public static void setAttribute(int[][] attribute) {
		ClusteredAffinityMatrix.attribute = attribute;
	}

	public static int[][] getClustering() {
		return clustering;
	}

	public static void setClustering(int[][] clustering) {
		ClusteredAffinityMatrix.clustering = clustering;
	}

	public static int[] getSites() {
		return sites;
	}

	public static void setSites(int[] sites) {
		ClusteredAffinityMatrix.sites = sites;
	}

	public void run() {
		array = new int[att];
		int loc = 0;
		Vector v = new Vector(); // Creates a default Vector
		int result = 0;
		for (int i = 0; i < att; i++) {
			ClusteredAffinityMatrix.clustering[i][0] = ClusteredAffinityMatrix.attribute[i][0];
			ClusteredAffinityMatrix.clustering[i][1] = ClusteredAffinityMatrix.attribute[i][1];
		}
		index = 2;
		int[] s = new int[3];
		while (index <= att - 1) {
			array = ClusteredAffinityMatrix.attribute[index];
			for (int i = 0; i <= index - 1; i++) {
				result = this.cont(i - 1, index, i);  // Returns the best placement
				s[0] = i - 1;
				s[1] = index;
				s[2] = i;
				Union u = new Union(result, s);
				// Adds the element to the end of the vector and increases its size by one
				v.addElement(u);
			}
			result = this.cont(index - 1, index, index + 1);
			s = new int[3];
			s[0] = index - 1;
			s[1] = index;
			s[2] = index + 1;
			Union u = new Union(result, s);
			v.addElement(u);
			u = this.maxCont(v);  // Checks for the maximum contribution value
			s = u.getOrder();
			loc = s[0] + 1;
			int[] temp = new int[att];
			for (int j = index; j >= loc; j--) {
				for (int m = 0; m < att; m++) {
					if (j - 1 < 0) {
						ClusteredAffinityMatrix.clustering[m][j] = 0;
					} else
						ClusteredAffinityMatrix.clustering[m][j] = ClusteredAffinityMatrix.clustering[m][j - 1];
				}
			}
			for (int i = 0; i < ClusteredAffinityMatrix.clustering.length; i++) {
				ClusteredAffinityMatrix.clustering[i][loc] = ClusteredAffinityMatrix.attribute[i][index];
			}
			index++;
			// Removes all elements from a vector and sets its size to zero
			v.removeAllElements();
		}
		int[] temp = new int[att];
		int[] tempPos = new int[att];
		int[][] tempC = new int[att][att];
		int[] original = new int[att];
		for (int i = 0; i < att; i++) {
			for (int j = 0; j < att; j++) {
				tempC[i][j] = ClusteredAffinityMatrix.clustering[i][j];
			}
		}
		for (int i = 0; i < att; i++) {
			for (int j = 0; j < att; j++) {
				temp[j] = ClusteredAffinityMatrix.clustering[j][i];
			}
			tempPos[i] = this.checkPosV(temp);
		}
		for (int i = 0; i < att; i++) {
			original[i] = i;
		}
		for (int i = 0; i < att; i++) {
			if (tempPos[i] != original[i]) {
				int pos1 = this.checkPosH(tempC[tempPos[i]]);
				int pos2 = this.checkPosH(tempC[original[i]]);
				for (int j = 0; j < att; j++) {
					ClusteredAffinityMatrix.clustering[pos1][j] = tempC[original[i]][j];
					ClusteredAffinityMatrix.clustering[pos2][j] = tempC[tempPos[i]][j];
				}
				int t = original[pos1];
				original[pos1] = original[pos2];
				original[pos2] = t;
			}
		}
	}

	// Calculates the contribution and thus the best placement of the element
	public int cont(int ai, int ak, int aj) {
		return 2 * bond(ai, ak) + 2 * bond(ak, aj) - 2 * bond(ai, aj);
	}

	// Computes the bond value which is used to calculate the contribution of elements
	public int bond(int ax, int ay) {
		if (ax < 0 || ay < 0 || ax > att - 1 || ay > att - 1) {
			return 0;
		}
		int result = 0;
		if (ax == index) {
			for (int i = 0; i < att; i++) {
				result += array[i] * ClusteredAffinityMatrix.clustering[i][ay];
			}
			return result;
		}
		if (ay == index) {
			for (int i = 0; i < att; i++) {
				result += ClusteredAffinityMatrix.clustering[i][ax] * array[i];
			}
		}
		for (int i = 0; i < att; i++) {
			result += ClusteredAffinityMatrix.clustering[i][ax] * ClusteredAffinityMatrix.clustering[i][ay];
		}
		return result;
	}

	public Union maxCont(Vector v) {
		// Gets the element at that position and assigng its value to 'max'
		int max = ((Union) v.elementAt(0)).getValue();
		for (int i = 1; i < v.size(); i++) {
			if (max < ((Union) v.elementAt(i)).getValue()) {
				max = ((Union) v.elementAt(i)).getValue();
			}
		}
		for (int i = 0; i < v.size(); i++) {
			if (max == ((Union) v.elementAt(i)).getValue()) {
				return (Union) v.elementAt(i);
			}
		}
		return null;
	}

	public int checkPosV(int[] array) {
		boolean same = false;
		int[] temp = new int[att];
		for (int i = 0; i < att; i++) {
			for (int j = 0; j < att; j++) {
				temp[j] = ClusteredAffinityMatrix.attribute[j][i];
			}
			for (int k = 0; k < att; k++) {
				if (array[k] == temp[k]) {
					same = true;
					continue;
				} else {
					same = false;
					break;
				}
			}
			if (same == true) return i;
		}
		return -1;
	}

	public int checkPosH(int[] array) {
		boolean same = false;
		for (int i = 0; i < att; i++) {
			for (int j = 0; j < att; j++) {
				if (array[j] == ClusteredAffinityMatrix.clustering[i][j]) {
					same = true;
					continue;
				} else {
					same = false;
					break;
				}
			}
			if (same == true) return i;
		}
		return -1;
	}

	class Union {
		private int contValue;
		private int[] ordering;

		public Union(int v, int[] s) {
			this.contValue = v;
			this.ordering = s;
		}

		public int[] getOrder() {
			return this.ordering;
		}

		public int getValue() {
			return this.contValue;
		}
	}
}