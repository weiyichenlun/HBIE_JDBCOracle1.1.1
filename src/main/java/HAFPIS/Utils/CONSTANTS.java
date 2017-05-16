package HAFPIS.Utils;

/**
 *
 * Created by ZP on 2017/5/15.
 */
public class CONSTANTS {
    public static final int[] srchOrder = new int[]{0, 4, 5, 9};
    public static final int[] feaOrder = new int[]{0, 2, 1, 3};

    public static final int FPTT = 1;
    public static final int FPLT = 2;
    public static final int FPTL = 3;
    public static final int FPLL = 4;
    public static final int FPTTLT = 5;
    public static final int FPTLLL = 6;
    public static final int PPTT = 7;
    public static final int PPLT = 8;
    public static final int PPTL = 9;
    public static final int PPLL = 10;
    public static final int PPTTLT = 11;
    public static final int PPTLLL = 12;
    public static final int FACE = 13;
    public static final int IRIS = 14;

    public static final int FpFeatureSize = 3072;
    public static final int[] PalmFeatureSize = new int[]{12288, 12288, 8192, 8192};
    public static final int FacefeatureSize = 1580;
    public static final int IrisFeatureSize = 2400;
    public static final int MAXCANDS = 100;
}
