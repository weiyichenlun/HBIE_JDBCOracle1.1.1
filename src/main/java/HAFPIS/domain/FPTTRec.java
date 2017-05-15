package HAFPIS.domain;

/**
 * Created by ZP on 2017/5/15.
 */
public class FPTTRec extends Rec<FPTTRec> {

    public float[] rpscores;
    public float[] fpscores;

    public FPTTRec() {
        rpscores = new float[10];
        fpscores = new float[10];
    }

}
