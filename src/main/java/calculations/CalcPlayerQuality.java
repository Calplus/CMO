package calculations;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class CalcPlayerQuality {

    private static final int LEAGUE_FLOOR[] = {0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 6, 8, 11, 14, 17, 21, 25, 25};



    /**
     * Creates a cumulative sum matrix with leading 0 and trailing total
     */
    public static int[][] createCumulativeMatrix(JsonArray datasets) {
        int[][] matrix = new int[datasets.size() + 7][];
        
        for (int i = 0; i < datasets.size() + 7; i++) {
            if (i < 7) {
                matrix[i] = new int[]{0};
            } else {
                JsonObject dataset = datasets.get(i - 7).getAsJsonObject();
                JsonArray dataArray = dataset.getAsJsonArray("data");
                matrix[i] = new int[dataArray.size() + 1]; // +1 for leading 0 and trailing sum
                
                matrix[i][0] = 0; // Leading zero
                int cumulativeSum = 0;
                
                for (int j = 0; j < dataArray.size(); j++) {
                    if (j < LEAGUE_FLOOR[i]-1) {
                        matrix[i][j+1] = 0;
                    } else {
                        cumulativeSum += dataArray.get(j).getAsInt();
                        matrix[i][j+1] = cumulativeSum;
                    }
                }
            }
        }
        
        return matrix;
    }



    /**
     * Creates the base score matrix from the cumulative sum matrix
     */
    public static double[][] createBaseScoreMatrix(int[][] culmulativeMatrix) {
        double[][] matrix = new double[culmulativeMatrix.length][culmulativeMatrix[7].length];
        
        for (int i = 7; i < matrix.length; i++) {
            int maxValue = culmulativeMatrix[i][culmulativeMatrix[i].length - 1];
            int pct25 = maxValue / 4;
            int pct99 = ((maxValue / 100) * 99) - pct25;

            for (int j = 1; j < matrix[i].length; j++) {
                if (culmulativeMatrix[i][j-1] <= pct25 ||j <= LEAGUE_FLOOR[i]) {
                    matrix[i][j] = 0;
                } else {
                    double score = (((double) culmulativeMatrix[i][j-1] - pct25) / pct99) * 100;
                    if (score >= 100) score = 100;
                    matrix[i][j] = score;
                }
            }
        }
        
        return matrix;
    }
}
