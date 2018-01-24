package HAFPIS.domain;

import java.io.Serializable;

/**
 * Created by ZP on 2017/5/15.
 */
public class FPTTRec extends Rec<FPTTRec> implements Serializable {

    private static final long serialVersionUID = 1611229844832755558L;
    public int[] rpscores;
    public int[] fpscores;

    public FPTTRec() {
        position = 0;//在指纹查重中无用，置为0
        rpscores = new int[10];
        fpscores = new int[10];
    }

}
