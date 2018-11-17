package cs522.part5;

public class Util {
	public static double normalisedDouble(String n1, double minValue, double maxValue) {
		return (Double.parseDouble(n1) - minValue) / (maxValue - minValue);
	}

	public static double nominalDistance(String t1, String t2) {
		if (t1.equals(t2)) {
			return 0;
		} else {
			return 1;
		}
	}


	public static double squaredDistance(double n1) {
		return Math.pow(n1, 2);
	}

	public static double totalSquaredDistance(double R1, double R2, String R3,
			String R4, double R5, double S1, double S2, String S3, String S4,
			double S5) {
		double ageDifference = S1 - R1;
		double incomeDifference = S2 - R2;
		double statusDifference = nominalDistance(S3, R3);
		double genderDifference = nominalDistance(S4, R4);
		double childrenDifference = S5 - R5;

		return squaredDistance(ageDifference)
				+ squaredDistance(incomeDifference) + statusDifference
				+ genderDifference + squaredDistance(childrenDifference);
	}
}
