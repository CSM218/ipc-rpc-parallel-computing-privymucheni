package pdc;

import java.util.Random;

/**
 * Utility class for generating and manipulating matrices.
 * Provides helper methods for creating test and example matrices.
 */
public class MatrixGenerator {

    private static final Random random = new Random();

    /**
     * Generates a random matrix of specified dimensions.
     * 
     * @param rows     number of rows
     * @param cols     number of columns
     * @param maxValue maximum value for matrix elements (exclusive)
     * @return a randomly generated matrix
     */
    public static int[][] generateRandomMatrix(int rows, int cols, int maxValue) {
        int[][] matrix = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                matrix[i][j] = random.nextInt(maxValue);
            }
        }
        return matrix;
    }

    /**
     * Generates an identity matrix of specified size.
     * 
     * @param size the dimension of the identity matrix
     * @return an identity matrix
     */
    public static int[][] generateIdentityMatrix(int size) {
        int[][] matrix = new int[size][size];
        for (int i = 0; i < size; i++) {
            matrix[i][i] = 1;
        }
        return matrix;
    }

    /**
     * Generates a matrix filled with a specific value.
     * 
     * @param rows  number of rows
     * @param cols  number of columns
     * @param value the value to fill the matrix with
     * @return a matrix filled with the specified value
     */
    public static int[][] generateFilledMatrix(int rows, int cols, int value) {
        int[][] matrix = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                matrix[i][j] = value;
            }
        }
        return matrix;
    }

    /**
     * Prints a matrix to console in a readable format.
     * 
     * @param matrix the matrix to print
     * @param label  optional label for the matrix
     */
    public static void printMatrix(int[][] matrix, String label) {
        if (label != null && !label.isEmpty()) {
            System.out.println(label);
        }
        for (int[] row : matrix) {
            for (int val : row) {
                System.out.printf("%6d ", val);
            }
            System.out.println();
        }
    }

    /**
     * Prints a matrix to console without a label.
     * 
     * @param matrix the matrix to print
     */
    public static void printMatrix(int[][] matrix) {
        printMatrix(matrix, "");
    }

    /**
     * Parse a matrix from the wire format used in the autograder:
     * rows separated by backslash '\\' and columns by comma ','
     * Example: "1,2\\3,4"
     */
    public static int[][] parse(String s) {
        if (s == null || s.isEmpty())
            return new int[0][0];
        String[] rows = s.split("\\\\");
        int n = rows.length;
        int m = rows[0].isEmpty() ? 0 : rows[0].split(",").length;
        int[][] mat = new int[n][m];
        for (int i = 0; i < n; i++) {
            if (rows[i].isEmpty())
                continue;
            String[] cols = rows[i].split(",");
            for (int j = 0; j < cols.length; j++) {
                try {
                    mat[i][j] = Integer.parseInt(cols[j]);
                } catch (NumberFormatException e) {
                    mat[i][j] = 0;
                }
            }
        }
        return mat;
    }

    /**
     * Serialize a matrix to the wire-format used by the autograder.
     */
    public static String serialize(int[][] a) {
        if (a == null)
            return "";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < a.length; i++) {
            if (i > 0)
                sb.append('\\');
            for (int j = 0; j < a[i].length; j++) {
                if (j > 0)
                    sb.append(',');
                sb.append(a[i][j]);
            }
        }
        return sb.toString();
    }

    /**
     * Multiply two matrices (deterministic, no parallelism here).
     */
    public static int[][] multiply(int[][] a, int[][] b) {
        if (a == null || b == null)
            return new int[0][0];
        int n = a.length;
        int m = b[0].length;
        int common = a[0].length;
        int[][] res = new int[n][m];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < m; j++) {
                long sum = 0;
                for (int k = 0; k < common; k++)
                    sum += (long) a[i][k] * b[k][j];
                res[i][j] = (int) sum;
            }
        }
        return res;
    }

    /**
     * Compare two matrices for equality.
     */
    public static boolean equals(int[][] a, int[][] b) {
        if (a == b)
            return true;
        if (a == null || b == null)
            return false;
        if (a.length != b.length)
            return false;
        for (int i = 0; i < a.length; i++) {
            if (a[i].length != b[i].length)
                return false;
            for (int j = 0; j < a[i].length; j++) {
                if (a[i][j] != b[i][j])
                    return false;
            }
        }
        return true;
    }
}
