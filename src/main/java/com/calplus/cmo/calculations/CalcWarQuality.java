package com.calplus.cmo.calculations;

public class CalcWarQuality {
    private static final double[][] CWL_ATTACKS_USED_MODIFIER = {
    // Atks:  0  |  1  |  2  |  3  |  4  |  5  |  6  |  7  |
            {0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00}, // In 0 wars
            {0.00, 0.05, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00}, // In 1 war
            {0.00, 0.00, 0.20, 0.00, 0.00, 0.00, 0.00, 0.00}, // In 2 wars
            {0.00, 0.00, 0.00, 0.45, 0.00, 0.00, 0.00, 0.00}, // In 3 wars
            {0.00, 0.00, 0.01, 0.20, 0.60, 0.00, 0.00, 0.00}, // In 4 wars
            {0.00, 0.00, 0.05, 0.15, 0.40, 0.75, 0.00, 0.00}, // In 5 wars
            {0.00, 0.00, 0.00, 0.10, 0.35, 0.70, 0.90, 0.00}, // In 6 wars
            {0.00, 0.00, 0.00, 0.00, 0.15, 0.55, 0.85, 1.00}  // In 7 wars
    // Atks:  0  |  1  |  2  |  3  |  4  |  5  |  6  |  7  |
    };


    /**
     * Calculates war quality based on stars and percentage
     * @param stars Number of stars (0-3)
     * @param percentage Destruction percentage (0-100)
     * @return Calculated quality score
     */
    public static double calculateStarsPercentageQuality (int stars, int percentage) {
        switch(stars) {
            case 3:
                return 100.0;
            case 2:
                if (percentage >= 90) {
                    return percentage;
                } else if (80 >= percentage && percentage <= 89) {
                    return 90 - (90 - percentage) * 1.4;
                } else if (50 >= percentage && percentage <= 79) {
                    return 76 - (80 - percentage) * 1.6;
                } else {
                    return 0.0;
                }
            case 1:
                if (percentage >= 80) {
                    return 60 - (100 - percentage) * 1.3;
                } else if (61 >= percentage && percentage <= 79) {
                    return 34 - (80 - percentage) * 1.7;
                } else if (31 >= percentage && percentage <= 49) {
                    return 28 - (50 - percentage) * 1.4;
                } else {
                    return 0.0;
                }
            case 0:
                return 0.0;
            default:
                return -1.0;
        }
    }



    /**
     * Calculates Town Hall modifier based on player's and opponent's TH levels
     * @param thLevel Player's Town Hall level
     * @param oppThLevel Opponent's Town Hall level
     * @return Calculated TH modifier
     */
    public static double calculateThModifier(int thLevel, int oppThLevel) {
        int thDiff = oppThLevel - thLevel;
        switch (thDiff) {
            case 2:
                return 1.25;
            case 1:
                return 1.10;
            case 0:
                return 1.00;
            case -1:
                return 0.80;
            case -2:
                return 0.50;
            default:
                if (thDiff >= 3) {
                    return 1.40;
                } else if (thDiff <= -3) {
                    return 0.20;
                } else {
                    return -1.0;
                }
        }
    }



    /**
     * Calculates CWL Town Hall modifier based on player's and opponent's TH levels
     * @param thLevel Player's Town Hall level
     * @param oppThLevel Opponent's Town Hall level
     * @return Calculated CWL TH modifier
     */
    public static double calculateCwlThModifier(int thLevel, int oppThLevel) {
        int thDiff = oppThLevel - thLevel;
        switch (thDiff) {
            case 3:
                return 1.55;
            case 2:
                return 1.35;
            case 1:
                return 1.15;
            case 0:
                return 1.00;
            case -1:
                return 0.90;
            case -2:
                return 0.70;
            default:
                if (thDiff >= 4) {
                    return 1.80;
                } else if (thDiff <= -3) {
                    return 0.50;
                } else {
                    return -1.0;
                }
        }
    }



    /**
     * Gets the CWL attacks used modifier based on number of wars and attacks used
     * @param warsCount Number of wars the player has participated in
     * @param attacksUsed Number of attacks used in the current war
     * @return Calculated CWL attacks used modifier
     */
    public static double calculateAttacksUsedModifier(int attacksUsed) {
        switch (attacksUsed) {
            case 2:
                return 1.0;
            case 1:
                return 0.4;
            case 0:
                return 0.0;
            default:
                return -1.0;
        }
    }



    /**
     * Gets the CWL attacks used modifier based on number of wars and attacks used
     * @param warsCount Number of wars the player has participated in
     * @param attacksUsed Number of attacks used in the current war
     * @return Calculated CWL attacks used modifier
     */
    public static double calculateCwlAttacksUsedModifier(int warsCount, int attacksUsed) {
        if (warsCount < 0 || warsCount >= CWL_ATTACKS_USED_MODIFIER.length || attacksUsed < 0 || attacksUsed >= CWL_ATTACKS_USED_MODIFIER[0].length) {
            return -1.0;
        }
        return CWL_ATTACKS_USED_MODIFIER[warsCount][attacksUsed];
    }



    /**
     * Calculates CWL mirror check modifier based on player's and opponent's positions
     * @param playerPos Player's position in the war lineup
     * @param oppPos Opponent's position in the war lineup
     * @return Calculated CWL mirror check modifier
     */
    public static double calculateCwlMirrorCheckModifier(int playerPos, int oppPos) {
        int score = playerPos - oppPos;

        switch(score) {
            case 0:
                return 1.0;
            default:
                if (score >= 5) {
                    return 0.25;
                } else if (score > 0 && score < 5) {
                    return 0.60;
                } else if (score < 0 && score > -5) {
                    return 0.40;
                } else if (score <= -5) {
                    return 0.10;
                } else {
                    return -1.0;
                }
        }
    }



    /**
     * Calculates the final war score
     * @param starsPctQuality Stars and percentage quality score
     * @param thModifier Town Hall modifier
     * @param attacksUsedModifier Attacks used modifier
     * @return Final war score
     */
    public static double calculateWarScore (double avgWarScore, double attacksUsedModifier) {
        return avgWarScore * attacksUsedModifier;
    }



    /**
     * Calculates the final CWL war score
     * @param starsPctQuality Stars and percentage quality score
     * @param thModifier Town Hall modifier
     * @param attacksUsedModifier Attacks used modifier
     * @param mirrorCheckModifier Mirror check modifier
     * @param isMirrorCheckEnabled Whether mirror check is enabled
     * @return Final CWL war score
     */
    public static double calculateCwlWarScore (double avgWarScore, double attacksUsedModifier, double mirrorCheckModifier, boolean isMirrorCheckEnabled) {
        if (!isMirrorCheckEnabled) {
            return avgWarScore * attacksUsedModifier;
        }

        return avgWarScore * attacksUsedModifier * mirrorCheckModifier;
    }
}
