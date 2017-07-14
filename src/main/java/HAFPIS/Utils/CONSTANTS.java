package HAFPIS.Utils;

/**
 *
 * Created by ZP on 2017/5/15.
 */
public class CONSTANTS {
    public static final int NCORES = Runtime.getRuntime().availableProcessors();

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

    public static final int FPTT1TOF = 20;
    public static final int FPLL1TOF = 21;
    public static final int PPTT1TOF = 22;
    public static final int PPLL1TOF = 23;
    public static final int FACE1TOF = 24;
    public static final int IRIS1TOF = 25;

    public static final int DBOP_TPP = 30;
    public static final int DBOP_LPP = 31;
    public static final int DBOP_PLP = 32;

    public static final int FpFeatureSize = 3072;
    public static final int[] PalmFeatureSize = new int[]{12288, 12288, 8192, 8192};
    public static final int FacefeatureSize = 1580;
    public static final int IrisFeatureSize = 2400;
    public static final int MAXCANDS = 100;

    public static int ppPos2Ora(int position) {
        int a = 0;
        switch (position) {
            case 0:
                a = 1;
                break;
            case 1:
                a = 6;
                break;
            case 2:
                a = 5;
                break;
            case 3:
                a = 10;
                break;
        }
        return a;
    }
}
