package HAFPIS.domain;

import java.io.Serializable;

/**
 * Created by ZP on 2017/5/15.
 */
public class PPTTRec extends Rec<PPTTRec> implements Serializable {
    private static final long serialVersionUID = -3148323057227415735L;
    public float ppscores[];

    public PPTTRec() {
        ppscores = new float[4];

    }
}
