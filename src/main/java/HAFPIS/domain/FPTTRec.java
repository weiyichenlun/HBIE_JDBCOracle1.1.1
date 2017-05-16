package HAFPIS.domain;

/**
 * Created by ZP on 2017/5/15.
 */
public class FPTTRec extends Rec<FPTTRec> {

    public float[] rpscores;
    public float[] fpscores;

    public FPTTRec() {
        position = 0;//在指纹查重中无用，置为0
        rpscores = new float[10];
        fpscores = new float[10];
    }

}
