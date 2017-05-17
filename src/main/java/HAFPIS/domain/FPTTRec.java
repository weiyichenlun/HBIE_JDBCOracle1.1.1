package HAFPIS.domain;

import java.io.Serializable;

/**
 * Created by ZP on 2017/5/15.
 */
public class FPTTRec extends Rec<FPTTRec> implements Serializable {

    private static final long serialVersionUID = 1611229844832755558L;
    public float[] rpscores;
    public float[] fpscores;

    public FPTTRec() {
        position = 0;//在指纹查重中无用，置为0
        rpscores = new float[10];
        fpscores = new float[10];
    }

}
